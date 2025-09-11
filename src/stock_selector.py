#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票选股策略模块
"""

import pandas as pd
import numpy as np
import os
from data_client import StockDataClient  # 修改为绝对导入
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

class StockSelector:
    """股票选股器"""
    
    def __init__(self, m1=14, m2=28, m3=57, m4=114, data_dir='stock_data'):
        """
        初始化选股器
        
        Args:
            m1, m2, m3, m4: 知行多空线的均线参数，默认为14, 28, 57, 114
            data_dir: 数据存储目录
        """
        self.data_client = StockDataClient()
        self.m1 = m1
        self.m2 = m2
        self.m3 = m3
        self.m4 = m4
        self.data_dir = data_dir
        
        # 线程锁，用于保护共享资源
        self._lock = threading.Lock()
        
    def load_stocks_data_parallel(self, analysis_stocks, analysis_date=None, max_workers=10, batch_size=50):
        """
        并行加载股票数据
        
        Args:
            analysis_stocks: 需要分析的股票列表 (DataFrame)
            analysis_date: 分析日期 (格式: YYYYMMDD)
            max_workers: 最大线程数
            batch_size: 批处理大小
            
        Returns:
            dict: 股票代码为键，数据为值的字典
        """
        print(f"🚀 开始并行加载 {len(analysis_stocks)} 只股票的数据...")
        print(f"⚙️  线程配置: 批处理大小={batch_size}, 最大线程数={max_workers}")
        
        # 确保获取足够的数据来计算最大周期的均线
        max_period = max(self.m1, self.m2, self.m3, self.m4)
        data_count = max_period + 30  # 多获取30天数据确保计算准确
        
        stocks_data = {}
        failed_stocks = []
        start_time = time.time()
        
        # 分批处理
        total_batches = (len(analysis_stocks) + batch_size - 1) // batch_size
        successful_count = 0
        
        for i in range(0, len(analysis_stocks), batch_size):
            batch_stocks = analysis_stocks.iloc[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            print(f"\n📦 处理第 {batch_num}/{total_batches} 批股票 ({len(batch_stocks)} 只)")
            print("-" * 40)
            
            batch_start_time = time.time()
            batch_data, batch_failed = self._load_batch_data_threaded(
                batch_stocks, analysis_date, data_count, max_workers
            )
            batch_end_time = time.time()
            
            # 合并结果
            with self._lock:
                stocks_data.update(batch_data)
                failed_stocks.extend(batch_failed)
                successful_count += len(batch_data)
            
            # 显示批次进度
            batch_success_rate = len(batch_data) / len(batch_stocks) * 100
            batch_time = batch_end_time - batch_start_time
            print(f"✅ 第{batch_num}批完成: {len(batch_data)}/{len(batch_stocks)} 成功 "
                  f"({batch_success_rate:.1f}%), 耗时: {batch_time:.1f}秒")
            
            # 显示总体进度
            total_progress = (i + len(batch_stocks)) / len(analysis_stocks) * 100
            elapsed_time = time.time() - start_time
            estimated_total_time = elapsed_time / total_progress * 100 if total_progress > 0 else 0
            remaining_time = estimated_total_time - elapsed_time
            
            print(f"📈 总进度: {successful_count}/{len(analysis_stocks)} "
                  f"({total_progress:.1f}%), 已用时: {elapsed_time:.1f}秒, "
                  f"预计剩余: {remaining_time:.1f}秒")
        
        # 打印总体统计
        total_time = time.time() - start_time
        success_rate = successful_count / len(analysis_stocks) * 100
        
        print(f"\n🎉 数据加载完成!")
        print("=" * 50)
        print(f"✅ 成功加载: {successful_count} 只股票 ({success_rate:.1f}%)")
        print(f"❌ 加载失败: {len(failed_stocks)} 只股票")
        print(f"⏱️  总耗时: {total_time:.1f} 秒")
        print(f"📈 平均速度: {successful_count/total_time:.1f} 只/秒")
        
        if failed_stocks:
            print(f"\n⚠️  失败股票列表 (前10个): {failed_stocks[:10]}")
            if len(failed_stocks) > 10:
                print(f"    ... 还有 {len(failed_stocks)-10} 只股票加载失败")
        
        return stocks_data
    
    def _load_batch_data_threaded(self, batch_stocks, analysis_date, data_count, max_workers):
        """
        使用多线程加载单批股票数据
        
        Args:
            batch_stocks: 股票批次数据
            analysis_date: 分析日期
            data_count: 需要的数据条数
            max_workers: 最大工作线程数
            
        Returns:
            tuple: (成功的股票数据字典, 失败的股票代码列表)
        """
        batch_data = {}
        failed_stocks = []
        
        # 创建线程池
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_stock = {}
            for idx, row in batch_stocks.iterrows():
                code = row['code']
                name = row.get('name', code)
                market = 1 if str(code).startswith('6') else 0
                
                future = executor.submit(
                    self._load_single_stock_safe, code, name, market, analysis_date, data_count
                )
                future_to_stock[future] = (code, name)
            
            # 收集结果
            completed_count = 0
            for future in as_completed(future_to_stock):
                code, name = future_to_stock[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    if result['success']:
                        batch_data[code] = result['data']
                        print(f"✅ {code} ({name}) - {len(result['data'])} 条记录 ({completed_count}/{len(batch_stocks)})")
                    else:
                        failed_stocks.append(code)
                        print(f"❌ {code} ({name}) - {result['error']} ({completed_count}/{len(batch_stocks)})")
                        
                except Exception as e:
                    failed_stocks.append(code)
                    print(f"❌ {code} ({name}) - 线程异常: {e} ({completed_count}/{len(batch_stocks)})")
        
        return batch_data, failed_stocks
    
    def _load_single_stock_safe(self, code, name, market, analysis_date, data_count):
        """
        安全地加载单只股票数据并计算技术指标
        
        Args:
            code: 股票代码
            name: 股票名称
            market: 市场标识
            analysis_date: 分析日期
            data_count: 需要的数据条数
            
        Returns:
            dict: 包含成功标志和数据的字典
        """
        try:
            # 为每个线程创建独立的客户端实例，避免共享冲突
            from data_client import StockDataClient
            thread_client = StockDataClient()
            
            # 获取股票数据
            data = self._get_stock_data_for_date_thread_safe(
                code, market, analysis_date, data_count, thread_client
            )
            
            if not data.empty:
                # 计算技术指标
                data_with_indicators = self.calculate_technical_indicators(data)
                
                # 验证指标计算是否成功
                required_indicators = ['J', 'ZF', 'ZF幅', '知行短期趋势线', '知行多空线']
                latest = data_with_indicators.iloc[-1]
                
                # 检查必要指标是否都存在且不为NaN
                all_valid = all(
                    indicator in latest.index and pd.notna(latest[indicator])
                    for indicator in required_indicators
                )
                
                if all_valid:
                    return {
                        'success': True,
                        'data': data_with_indicators
                    }
                else:
                    return {
                        'success': False,
                        'error': '技术指标计算失败'
                    }
            else:
                return {
                    'success': False,
                    'error': '无数据'
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _get_stock_data_for_date_thread_safe(self, code, market, analysis_date, data_count, thread_client):
        """
        线程安全的股票数据获取方法
        
        Args:
            code: 股票代码
            market: 市场标识
            analysis_date: 分析日期
            data_count: 需要的数据条数
            thread_client: 线程专用的客户端实例
            
        Returns:
            DataFrame: 股票数据
        """
        try:
            # 尝试从本地数据获取（本地文件读取是线程安全的）
            local_file = os.path.join(self.data_dir, "stocks", f"{code}.csv")
            if os.path.exists(local_file):
                # 读取本地数据
                df = pd.read_csv(local_file)
                if not df.empty and '日期' in df.columns:
                    # 确保日期格式正确
                    df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
                    # 筛选到指定日期及之前的数据
                    if analysis_date:
                        target_date = pd.to_datetime(analysis_date, format='%Y%m%d')
                        df = df[df['日期'] <= target_date]
                    
                    if len(df) >= data_count:
                        # 返回最近的data_count条数据
                        return df.tail(data_count).reset_index(drop=True)
                    elif len(df) > 0:
                        # 如果数据不够，返回所有可用数据
                        return df.reset_index(drop=True)
            
            # 如果本地数据不存在或不足，使用在线数据（使用线程专用客户端）
            data = thread_client.get_daily_data(code, market=market, count=data_count)
            
            # 如果指定了分析日期，需要筛选数据
            if not data.empty and analysis_date:
                try:
                    # 确保日期列存在并转换格式
                    if '日期' in data.columns:
                        data['日期'] = pd.to_datetime(data['日期'], format='%Y%m%d', errors='coerce')
                        target_date = pd.to_datetime(analysis_date, format='%Y%m%d')
                        data = data[data['日期'] <= target_date]
                except Exception:
                    # 日期筛选失败时返回原始数据
                    pass
            
            return data
            
        except Exception:
            return pd.DataFrame()

    def get_stock_data_for_date(self, code, market, analysis_date, data_count):
        """
        获取指定日期及之前的股票数据（保留原方法用于兼容性）
        
        Args:
            code: 股票代码
            market: 市场 (0: 深圳, 1: 上海)
            analysis_date: 分析日期 (格式: YYYYMMDD)
            data_count: 需要的数据条数
        """
        return self._get_stock_data_for_date_thread_safe(
            code, market, analysis_date, data_count, self.data_client
        )
        """
        获取指定日期及之前的股票数据
        
        Args:
            code: 股票代码
            market: 市场 (0: 深圳, 1: 上海)
            analysis_date: 分析日期 (格式: YYYYMMDD)
            data_count: 需要的数据条数
        """
        try:
            # 尝试从本地数据获取
            local_file = os.path.join(self.data_dir, "stocks", f"{code}.csv")
            if os.path.exists(local_file):
                # 读取本地数据
                df = pd.read_csv(local_file)
                if not df.empty and '日期' in df.columns:
                    # 确保日期格式正确
                    df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
                    # 筛选到指定日期及之前的数据
                    target_date = pd.to_datetime(analysis_date, format='%Y%m%d')
                    df = df[df['日期'] <= target_date]
                    
                    if len(df) >= data_count:
                        # 返回最近的data_count条数据
                        return df.tail(data_count).reset_index(drop=True)
                    elif len(df) > 0:
                        # 如果数据不够，返回所有可用数据
                        print(f"⚠️  {code} 本地数据不足，期望{data_count}条，实际{len(df)}条")
                        return df.reset_index(drop=True)
            
            # 如果本地数据不存在或不足，使用在线数据
            print(f"📡 {code} 使用在线数据 (目标日期: {analysis_date})")
            data = self.data_client.get_daily_data(code, market=market, count=data_count)
            
            # 如果指定了分析日期，需要筛选数据
            if not data.empty and analysis_date:
                try:
                    # 确保日期列存在并转换格式
                    if '日期' in data.columns:
                        data['日期'] = pd.to_datetime(data['日期'], format='%Y%m%d', errors='coerce')
                        target_date = pd.to_datetime(analysis_date, format='%Y%m%d')
                        data = data[data['日期'] <= target_date]
                except Exception as e:
                    print(f"⚠️  {code} 日期筛选出错: {e}")
            
            return data
            
        except Exception as e:
            print(f"❌ 获取 {code} 数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, data):
        """
        计算技术指标
        
        Args:
            data: 股票数据DataFrame
        """
        if data.empty:
            return data
        
        df = data.copy()
        
        # 计算移动平均线（使用参数化的周期）
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df[f'MA{self.m1}'] = df['收盘'].rolling(window=self.m1).mean()
        df[f'MA{self.m2}'] = df['收盘'].rolling(window=self.m2).mean()
        df[f'MA{self.m3}'] = df['收盘'].rolling(window=self.m3).mean()
        df[f'MA{self.m4}'] = df['收盘'].rolling(window=self.m4).mean()
        
        # 计算KDJ指标
        low_min = df['最低'].rolling(window=9).min()
        high_max = df['最高'].rolling(window=9).max()
        
        # RSV = (CLOSE-LLV(LOW,9))/(HHV(HIGH,9)-LLV(LOW,9))*100
        df['RSV'] = (df['收盘'] - low_min) / (high_max - low_min) * 100
        df['RSV'] = df['RSV'].fillna(0)
        
        # K = SMA(RSV,3,1)
        df['K'] = df['RSV'].ewm(alpha=1/3, adjust=False).mean()
        
        # D = SMA(K,3,1)
        df['D'] = df['K'].ewm(alpha=1/3, adjust=False).mean()
        
        # J = 3*K-2*D
        df['J'] = 3 * df['K'] - 2 * df['D']
        
        # 计算涨幅
        df['ZF'] = (df['收盘'] / df['收盘'].shift(1) - 1) * 100
        
        # 计算振幅
        df['ZF幅'] = (df['最高'] - df['最低']) / df['收盘'].shift(1) * 100
        
        # 计算知行线指标
        # 知行短期趋势线 = EMA(EMA(CLOSE,10),10)
        ema10 = df['收盘'].ewm(span=10).mean()
        df['知行短期趋势线'] = ema10.ewm(span=10).mean()
        
        # 知行多空线 = (MA(CLOSE,M1)+MA(CLOSE,M2)+MA(CLOSE,M3)+MA(CLOSE,M4))/4
        df['知行多空线'] = (df[f'MA{self.m1}'] + df[f'MA{self.m2}'] + df[f'MA{self.m3}'] + df[f'MA{self.m4}']) / 4
        
        return df
    
    def b1_strategy(self, stocks_data, analysis_date=None):
        """
        B1策略选股
        
        B1策略条件:
        1. J <= 13
        2. 涨幅 >= -2 AND 涨幅 <= 1.8
        3. 振幅 <= 7
        4. 知行短期趋势线 > 知行多空线
        5. 收盘价 > 知行多空线
        
        Args:
            stocks_data: 股票数据字典
            analysis_date: 分析日期 (格式: YYYYMMDD)
        """
        selected_stocks = []
        
        print("正在执行B1策略筛选...")
        if analysis_date:
            analysis_date_str = pd.to_datetime(analysis_date, format='%Y%m%d').strftime('%Y年%m月%d日')
            print(f"📅 基于 {analysis_date_str} 的数据进行分析")
        print(f"筛选条件:")
        print(f"  1. J值 <= 13")
        print(f"  2. 涨幅 >= -2% AND 涨幅 <= 1.8%")
        print(f"  3. 振幅 <= 7%")
        print(f"  4. 知行短期趋势线 > 知行多空线")
        print(f"  5. 收盘价 > 知行多空线")
        print("-" * 50)
        
        for code, data in stocks_data.items():
            if data.empty:
                continue
                
            # 获取最新数据
            latest = data.iloc[-1]
            
            # 检查必要指标是否存在
            required_fields = ['J', 'ZF', 'ZF幅', '知行短期趋势线', '知行多空线', '收盘']
            if not all(field in latest.index for field in required_fields):
                print(f"跳过 {code}: 缺少必要的技术指标")
                continue
            
            # B1策略条件检查
            j_value = latest['J']
            zf = latest['ZF']
            zf_range = latest['ZF幅']
            short_trend = latest['知行短期趋势线']
            multi_trend = latest['知行多空线']
            close_price = latest['收盘']
            
            # 检查是否有NaN值
            if pd.isna(j_value) or pd.isna(zf) or pd.isna(zf_range) or pd.isna(short_trend) or pd.isna(multi_trend):
                print(f"跳过 {code}: 存在无效数据")
                continue
            
            # 条件1: J <= 13
            cond1 = j_value <= 13
            
            # 条件2: 涨幅 >= -2 AND 涨幅 <= 1.8
            cond2 = -2 <= zf <= 1.8
            
            # 条件3: 振幅 <= 7
            cond3 = zf_range <= 7
            
            # 条件4: 知行短期趋势线 > 知行多空线
            cond4 = short_trend > multi_trend
            
            # 条件5: 收盘价 > 知行多空线
            cond5 = close_price > multi_trend
            
            # 输出详细检查信息
            print(f"{code}: J={j_value:.2f} ZF={zf:.2f}% 振幅={zf_range:.2f}% 短期={short_trend:.2f} 多空={multi_trend:.2f}")
            print(f"  条件检查: J≤13:{cond1} 涨幅范围:{cond2} 振幅≤7:{cond3} 短期>多空:{cond4} 收盘>多空:{cond5}")
            
            # 所有条件都满足
            if cond1 and cond2 and cond3 and cond4 and cond5:
                print(f"✅ {code} 符合B1策略条件!")
                
                selected_stocks.append({
                    'code': code,
                    'name': code,  # 暂时用代码作为名称
                    'price': close_price,
                    'change_pct': zf,
                    'j_value': j_value,
                    'range_pct': zf_range,
                    'short_trend': short_trend,
                    'multi_trend': multi_trend
                })
            else:
                print(f"❌ {code} 不符合条件")
            
            print()  # 空行分隔
        
        # 转换为DataFrame（不需要排序，因为没有评分）
        if selected_stocks:
            df = pd.DataFrame(selected_stocks)
            print(f"🎯 B1策略筛选出 {len(df)} 只符合条件的股票")
            return df
        else:
            print("❌ B1策略未找到符合条件的股票")
            return pd.DataFrame()
    
    def save_to_blk_file(self, selected_stocks, filename, analysis_date=None):
        """
        保存选股结果为.blk文件 (通达信格式)
        
        Args:
            selected_stocks: 选出的股票DataFrame
            filename: 文件名
            analysis_date: 分析日期 (格式: YYYYMMDD)
        """
        if selected_stocks.empty:
            print("❌ 没有股票可保存")
            return None
        
        # 创建输出目录
        output_dir = "output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 生成文件路径，包含分析日期
        if analysis_date:
            date_str = analysis_date
        else:
            date_str = datetime.now().strftime("%Y%m%d")
        
        timestamp = datetime.now().strftime("%H%M%S")
        filepath = os.path.join(output_dir, f"{filename}_{date_str}_{timestamp}.blk")
        
        # 转换股票代码为7位格式（第一位是市场标识）
        blk_codes = []
        for _, row in selected_stocks.iterrows():
            code = str(row['code'])
            
            # 判断市场并添加市场标识
            if code.startswith('6'):  # 上海市场
                market_code = f"1{code}"
            elif code.startswith(('000', '002', '300')):  # 深圳市场
                market_code = f"0{code}"
            else:
                continue  # 跳过无法识别的代码
                
            blk_codes.append(market_code)
        
        # 写入文件
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                for code in blk_codes:
                    f.write(f"{code}\n")
            
            print(f"✅ 成功保存 {len(blk_codes)} 只股票到: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"❌ 保存文件失败: {e}")
            return None
    
    def run_stock_selection(self, strategy='b1', stock_count=None, stock_list=None, save_blk=True, analysis_date=None, max_workers=10, batch_size=50):
        """
        执行选股（支持多线程加速数据读取）
        
        Args:
            strategy: 策略类型 ('b1')
            stock_count: 分析股票数量，None表示分析所有股票
            stock_list: 指定的股票代码列表，如果指定则只分析这些股票
            save_blk: 是否保存为BLK文件
            analysis_date: 分析日期 (格式: YYYYMMDD)
            max_workers: 最大线程数（默认10）
            batch_size: 批处理大小（默认50）
        """
        print(f"开始执行{strategy}策略选股...")
        print(f"知行多空线参数: M1={self.m1}, M2={self.m2}, M3={self.m3}, M4={self.m4}")
        
        if analysis_date:
            analysis_date_str = pd.to_datetime(analysis_date, format='%Y%m%d').strftime('%Y年%m月%d日')
            print(f"📅 分析基准日期: {analysis_date_str}")
        
        # 如果指定了股票列表，直接使用
        if stock_list:
            print(f"🎯 指定股票模式：分析 {', '.join(stock_list)} 共{len(stock_list)}只股票")
            analysis_stocks = []
            for code in stock_list:
                # 构造股票信息
                analysis_stocks.append({
                    'code': code,
                    'name': f'股票{code}'  # 临时名称
                })
            analysis_stocks = pd.DataFrame(analysis_stocks)
        else:
            # 获取股票列表
            all_stock_list = self.data_client.get_stock_list()
            if all_stock_list.empty:
                print("无法获取股票列表")
                return pd.DataFrame()
            
            # 限制分析数量，避免请求过多，并过滤掉指数
            all_stock_list = all_stock_list[all_stock_list['name'].str.len() > 0]  # 确保有名称
            
            # 只选择真正的股票（过滤指数、ST股票等）
            real_stocks = all_stock_list[
                (~all_stock_list['name'].str.contains('指数|指|ETF|LOF|ST|退', na=False)) &
                (all_stock_list['code'].str.match(r'^(6\d{5}|000\d{3}|002\d{3}|300\d{3})$', na=False))
            ]
            
            if real_stocks.empty:
                print("未找到符合条件的股票")
                return pd.DataFrame()
                
            # 根据stock_count决定分析范围
            if stock_count is None:
                # 分析所有股票
                print(f"📊 全市场分析模式：共{len(real_stocks)}只股票")
                analysis_stocks = real_stocks
            else:
                # 分析指定数量的股票
                analysis_stocks = real_stocks.head(stock_count)
                print(f"📊 限量分析模式：分析前{len(analysis_stocks)}只股票")
        
        # 使用多线程并行加载股票数据和计算指标
        print(f"🔧 多线程配置: 批处理大小={batch_size}, 最大线程数={max_workers}")
        stocks_data = self.load_stocks_data_parallel(
            analysis_stocks, 
            analysis_date=analysis_date, 
            max_workers=max_workers, 
            batch_size=batch_size
        )
        
        # 执行选股策略
        if strategy == 'b1':
            selected = self.b1_strategy(stocks_data, analysis_date=analysis_date)
        else:
            print(f"未知策略: {strategy}")
            return pd.DataFrame()
        
        # 保存为BLK文件
        if save_blk and not selected.empty:
            self.save_to_blk_file(selected, f"{strategy.upper()}", analysis_date=analysis_date)
        
        return selected

if __name__ == "__main__":
    print("=" * 50)
    print("股票选股系统测试 - B1策略")
    print("=" * 50)
    
    # 使用您指定的参数
    selector = StockSelector(m1=14, m2=28, m3=57, m4=114)
    
    # 测试B1策略
    print("\n测试B1策略选股...")
    b1_stocks = selector.run_stock_selection(strategy='b1', stock_count=10, save_blk=True)
    
    if not b1_stocks.empty:
        print(f"\nB1策略选出 {len(b1_stocks)} 只股票:")
        print(b1_stocks.to_string(index=False))
    else:
        print("B1策略未选出股票")
    
    print("\n选股测试完成!")