#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A股全市场数据管理模块
支持全量获取、增量更新、本地存储
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import pickle
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from data_client import StockDataClient

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockDataManager:
    """A股数据管理器"""
    
    def __init__(self, data_dir="stock_data"):
        """
        初始化数据管理器
        
        Args:
            data_dir: 数据存储目录
        """
        self.data_dir = data_dir
        self.client = StockDataClient()
        self.metadata_file = os.path.join(data_dir, "metadata.json")
        
        # 创建数据目录结构
        self._create_directories()
        
        # 加载元数据
        self.metadata = self._load_metadata()
        
        # 线程锁，用于保护共享资源
        self._lock = threading.Lock()
        
        logger.info(f"数据管理器初始化完成，数据目录: {self.data_dir}")
    
    def _create_directories(self):
        """创建必要的目录结构"""
        directories = [
            self.data_dir,
            os.path.join(self.data_dir, "daily"),      # 日线数据
            os.path.join(self.data_dir, "stocks"),     # 个股数据
            os.path.join(self.data_dir, "backup"),     # 备份数据
            os.path.join(self.data_dir, "temp")        # 临时文件
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def _load_metadata(self):
        """加载元数据"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                logger.info("元数据加载成功")
                return metadata
            except Exception as e:
                logger.warning(f"元数据加载失败: {e}，将创建新的元数据")
        
        # 创建默认元数据
        return {
            "last_update": None,
            "stock_count": 0,
            "data_version": "1.0",
            "update_history": [],
            "failed_stocks": []
        }
    
    def _save_metadata(self):
        """保存元数据"""
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, ensure_ascii=False, indent=2)
            logger.debug("元数据保存成功")
        except Exception as e:
            logger.error(f"元数据保存失败: {e}")
    
    def _get_today_str(self):
        """获取今日日期字符串"""
        return datetime.now().strftime("%Y%m%d")
    
    def _is_trading_day(self, date=None):
        """
        判断是否为交易日（简化版，不考虑具体节假日）
        
        Args:
            date: 日期，默认为今天
        """
        if date is None:
            date = datetime.now()
        
        # 周末不是交易日
        if date.weekday() >= 5:  # 5=周六, 6=周日
            return False
        
        # 简单判断，实际应该结合交易所日历
        return True
    
    def _should_update_data(self):
        """判断是否需要更新数据"""
        if not self.metadata.get("last_update"):
            return True, "首次获取数据"
        
        try:
            last_update = datetime.strptime(self.metadata["last_update"], "%Y%m%d")
            today = datetime.now()
            
            # 如果不是交易日，检查最近的交易日
            if not self._is_trading_day(today):
                # 查找最近的交易日
                check_date = today
                for _ in range(7):  # 最多往前找7天
                    check_date -= timedelta(days=1)
                    if self._is_trading_day(check_date):
                        if last_update.date() >= check_date.date():
                            return False, "数据已是最新（非交易日）"
                        else:
                            return True, f"需要更新到最近交易日: {check_date.strftime('%Y%m%d')}"
                return False, "无法确定最近交易日"
            
            # 如果是交易日，判断是否需要更新
            if last_update.date() < today.date():
                return True, f"数据过期，最后更新: {self.metadata['last_update']}"
            
            return False, "数据已是最新"
            
        except Exception as e:
            logger.warning(f"判断更新状态失败: {e}")
            return True, "无法判断更新状态，强制更新"
    
    def get_all_stocks(self, force_update=False):
        """
        获取全市场股票列表
        
        Args:
            force_update: 强制更新
        """
        today_str = self._get_today_str()
        stock_list_file = os.path.join(self.data_dir, f"stock_list_{today_str}.pkl")
        
        # 检查是否需要更新
        if not force_update and os.path.exists(stock_list_file):
            try:
                logger.info(f"📂 加载本地股票列表: {stock_list_file}")
                stocks = pd.read_pickle(stock_list_file)
                logger.info(f"✅ 本地股票列表加载成功，共 {len(stocks)} 只股票")
                return stocks
            except Exception as e:
                logger.warning(f"本地股票列表加载失败: {e}，将重新获取")
        
        logger.info("🔄 从服务器获取最新股票列表...")
        stocks = self.client.get_stock_list()
        
        if not stocks.empty:
            try:
                # 保存到本地
                stocks.to_pickle(stock_list_file)
                logger.info(f"💾 股票列表已保存: {stock_list_file}")
                logger.info(f"📊 共获取 {len(stocks)} 只股票")
                
                # 更新元数据
                self.metadata["stock_count"] = len(stocks)
                self._save_metadata()
                
            except Exception as e:
                logger.error(f"保存股票列表失败: {e}")
        else:
            logger.error("获取股票列表失败")
        
        return stocks
    
    def download_stock_data(self, code, market, days=300, force_update=False):
        """
        下载单只股票数据（极简版本）
        
        Args:
            code: 股票代码
            market: 市场代码
            days: 获取天数
            force_update: 强制更新
        """
        # 直接获取数据，不做本地缓存检查
        data = self.client.get_daily_data(code, market=market, count=days)
        
        if not data.empty:
            logger.debug(f"✅ {code}: {len(data)} 条记录")
        else:
            logger.warning(f"❌ {code}: 无数据")
        
        return data
    
    def download_all_market_data(self, force_update=False, max_stocks=None, batch_size=50, max_workers=10, end_date=None):
        """
        下载全市场数据
        
        Args:
            force_update: 强制更新
            max_stocks: 最大股票数量（用于测试）
            batch_size: 批处理大小
            max_workers: 最大线程数
            end_date: 结束日期 (格式: YYYYMMDD)
        """
        print("🚀 开始获取A股全市场数据")
        print("=" * 60)
        
        if end_date:
            end_date_str = pd.to_datetime(end_date, format='%Y%m%d').strftime('%Y年%m月%d日')
            print(f"📅 数据获取到: {end_date_str}")
        
        # 检查是否需要更新
        need_update, reason = self._should_update_data()
        if not need_update and not force_update and not end_date:
            print(f"ℹ️  {reason}")
            existing_data = self._load_existing_data()
            if existing_data:
                return existing_data
        
        print(f"📅 更新原因: {reason}")
        
        # 获取股票列表
        stocks = self.get_all_stocks(force_update)
        if stocks.empty:
            print("❌ 无法获取股票列表")
            return {}
        
        # 限制股票数量（用于测试）
        if max_stocks and max_stocks > 0:
            stocks = stocks.head(max_stocks)
            print(f"🧪 测试模式: 仅获取前 {max_stocks} 只股票")
        
        print(f"📊 准备获取 {len(stocks)} 只股票的数据")
        print(f"⚙️  批处理大小: {batch_size}, 线程数: {max_workers}")
        
        # 开始时间
        start_time = time.time()
        
        # 分批处理
        all_data = {}
        failed_stocks = []
        successful_count = 0
        
        total_batches = (len(stocks) + batch_size - 1) // batch_size
        
        for i in range(0, len(stocks), batch_size):
            batch_stocks = stocks.iloc[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            print(f"\n📦 处理第 {batch_num}/{total_batches} 批股票 ({len(batch_stocks)} 只)")
            print("-" * 40)
            
            batch_start_time = time.time()
            batch_data, batch_failed = self._process_batch_threaded(batch_stocks, force_update, max_workers, end_date)
            batch_end_time = time.time()
            
            # 合并结果
            all_data.update(batch_data)
            failed_stocks.extend(batch_failed)
            successful_count += len(batch_data)
            
            # 显示批次进度
            batch_success_rate = len(batch_data) / len(batch_stocks) * 100
            batch_time = batch_end_time - batch_start_time
            avg_time_per_stock = batch_time / len(batch_stocks)
            print(f"✅ 第{batch_num}批完成: {len(batch_data)}/{len(batch_stocks)} 成功 "
                  f"({batch_success_rate:.1f}%), 耗时: {batch_time:.1f}秒 "
                  f"(平均 {avg_time_per_stock:.2f}秒/只)")
            
            # 显示总体进度
            total_progress = (i + len(batch_stocks)) / len(stocks) * 100
            elapsed_time = time.time() - start_time
            estimated_total_time = elapsed_time / total_progress * 100 if total_progress > 0 else 0
            remaining_time = estimated_total_time - elapsed_time
            
            print(f"📈 总进度: {successful_count}/{len(stocks)} "
                  f"({total_progress:.1f}%), 已用时: {elapsed_time:.1f}秒, "
                  f"预计剩余: {remaining_time:.1f}秒")
            
            # 删除批次间休息，最大化下载速度
        
        # 保存全量数据
        if end_date:
            data_date = end_date
        else:
            data_date = self._get_today_str()
        
        all_data_file = os.path.join(self.data_dir, f"all_market_data_{data_date}.pkl")
        
        try:
            with open(all_data_file, 'wb') as f:
                pickle.dump(all_data, f)
            logger.info(f"全市场数据已保存: {all_data_file}")
        except Exception as e:
            logger.error(f"保存全市场数据失败: {e}")
        
        # 更新元数据
        end_time = time.time()
        total_time = end_time - start_time
        
        update_date = data_date  # 使用数据日期作为更新标识
        
        self.metadata["last_update"] = update_date
        self.metadata["successful_stocks"] = successful_count
        self.metadata["failed_stocks"] = len(failed_stocks)
        self.metadata["update_history"].append({
            "date": update_date,
            "total_stocks": len(stocks),
            "successful": successful_count,
            "failed": len(failed_stocks),
            "duration": total_time,
            "is_historical": bool(end_date),  # 标记是否为历史数据
            "target_date": end_date if end_date else update_date
        })
        
        # 保留最近10次更新记录
        if len(self.metadata["update_history"]) > 10:
            self.metadata["update_history"] = self.metadata["update_history"][-10:]
        
        self._save_metadata()
        
        # 打印统计信息
        success_rate = successful_count / len(stocks) * 100
        print(f"\n🎉 数据获取完成!")
        print("=" * 60)
        print(f"✅ 成功获取: {successful_count} 只股票 ({success_rate:.1f}%)")
        print(f"❌ 获取失败: {len(failed_stocks)} 只股票")
        print(f"⏱️  总耗时: {total_time:.1f} 秒")
        print(f"📈 平均速度: {successful_count/total_time:.1f} 只/秒")
        print(f"💾 数据文件: {all_data_file}")
        
        if failed_stocks:
            print(f"\n⚠️  失败股票列表 (前10个): {failed_stocks[:10]}")
            if len(failed_stocks) > 10:
                print(f"    ... 还有 {len(failed_stocks)-10} 只股票获取失败")
        
        return all_data
    
    def _process_batch(self, batch_stocks, force_update):
        """处理单批股票"""
        batch_data = {}
        failed_stocks = []
        
        for idx, row in batch_stocks.iterrows():
            code = row['code']
            name = row.get('name', code)
            market = 1 if str(code).startswith('6') else 0
            
            try:
                data = self.download_stock_data(code, market, days=300, force_update=force_update)
                if not data.empty:
                    batch_data[code] = {
                        'data': data,
                        'info': {
                            'code': code,
                            'name': name,
                            'market': market,
                            'update_time': datetime.now().isoformat(),
                            'data_count': len(data)
                        }
                    }
                    print(f"✅ {code} ({name}) - {len(data)} 条记录")
                else:
                    failed_stocks.append(code)
                    print(f"❌ {code} ({name}) - 数据为空")
                
            except Exception as e:
                failed_stocks.append(code)
                print(f"❌ {code} ({name}) - 获取失败: {e}")
                continue
            
            # 删除请求间隔，最大化下载速度
        
        return batch_data, failed_stocks
    
    def _process_batch_threaded(self, batch_stocks, force_update, max_workers=10, end_date=None):
        """
        使用多线程处理单批股票
        
        Args:
            batch_stocks: 股票批次数据
            force_update: 强制更新
            max_workers: 最大工作线程数
            end_date: 结束日期 (格式: YYYYMMDD)
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
                
                future = executor.submit(self._download_single_stock_safe, code, name, market, force_update, end_date)
                future_to_stock[future] = (code, name)
            
            # 收集结果
            completed_count = 0
            for future in as_completed(future_to_stock):
                code, name = future_to_stock[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    if result['success']:
                        with self._lock:  # 保护共享数据
                            batch_data[code] = result['data']
                        print(f"✅ {code} ({name}) - {result['data']['info']['data_count']} 条记录 ({completed_count}/{len(batch_stocks)})")
                    else:
                        with self._lock:
                            failed_stocks.append(code)
                        print(f"❌ {code} ({name}) - {result['error']} ({completed_count}/{len(batch_stocks)})")
                        
                except Exception as e:
                    with self._lock:
                        failed_stocks.append(code)
                    print(f"❌ {code} ({name}) - 线程异常: {e} ({completed_count}/{len(batch_stocks)})")
        
        return batch_data, failed_stocks
    
    def _download_single_stock_safe(self, code, name, market, force_update, end_date=None):
        """
        安全地下载单只股票数据（每个线程使用独立客户端）
        
        Args:
            code: 股票代码
            name: 股票名称  
            market: 市场标识
            force_update: 强制更新
            end_date: 结束日期 (格式: YYYYMMDD)
            
        Returns:
            dict: 包含成功标志和数据的字典
        """
        try:
            # 为每个线程创建独立的客户端实例，避免共享冲突
            from data_client import StockDataClient
            thread_client = StockDataClient()
            
            # 只重试2次，减少等待时间
            for attempt in range(2):
                try:
                    # 根据重试次数调整参数
                    if attempt == 0:
                        # 第一次：标准参数
                        data = thread_client.get_daily_data(code, market=market, count=300)
                    else:
                        # 第二次：减少数据量，更快获取
                        data = thread_client.get_daily_data(code, market=market, count=100)
                    
                    # 如果指定了结束日期，需要筛选数据
                    if not data.empty and end_date:
                        try:
                            # 确保日期列存在并转换格式
                            if '日期' in data.columns:
                                data['日期'] = pd.to_datetime(data['日期'], format='%Y%m%d', errors='coerce')
                                target_date = pd.to_datetime(end_date, format='%Y%m%d')
                                data = data[data['日期'] <= target_date]
                        except Exception as e:
                            print(f"⚠️  {code} 日期筛选出错: {e}")
                    
                    if not data.empty:
                        # 快速验证数据质量
                        if '收盘' in data.columns and len(data) > 0:
                            latest_close = data['收盘'].iloc[-1]
                            if pd.notna(latest_close) and latest_close > 0:
                                stock_info = {
                                    'data': data,
                                    'info': {
                                        'code': code,
                                        'name': name,
                                        'market': market,
                                        'update_time': datetime.now().isoformat(),
                                        'data_count': len(data),
                                        'end_date': end_date if end_date else datetime.now().strftime('%Y%m%d')
                                    }
                                }
                                
                                return {
                                    'success': True,
                                    'data': stock_info
                                }
                
                    # 重试前极短等待，多线程下减少冲突
                    if attempt == 0:
                        time.sleep(0.01)  # 只等待10毫秒
                        
                except Exception as e:
                    # 只在第一次失败时短暂等待
                    if attempt == 0:
                        time.sleep(0.01)
                    continue
            
            return {
                'success': False,
                'error': '无数据'
            }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _load_existing_data(self):
        """加载现有数据"""
        today_str = self._get_today_str()
        all_data_file = os.path.join(self.data_dir, f"all_market_data_{today_str}.pkl")
        
        if os.path.exists(all_data_file):
            try:
                logger.info(f"📂 加载现有数据: {all_data_file}")
                with open(all_data_file, 'rb') as f:
                    data = pickle.load(f)
                logger.info(f"✅ 现有数据加载成功，共 {len(data)} 只股票")
                return data
            except Exception as e:
                logger.warning(f"现有数据加载失败: {e}")
        
        return {}
    
    def get_market_data(self, date=None):
        """
        获取指定日期的市场数据
        
        Args:
            date: 指定日期，格式YYYYMMDD，默认今天
        """
        if date is None:
            date = self._get_today_str()
        
        data_file = os.path.join(self.data_dir, f"all_market_data_{date}.pkl")
        
        if os.path.exists(data_file):
            try:
                with open(data_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"加载数据失败 {date}: {e}")
        
        return {}
    
    def list_available_data(self):
        """列出可用的数据文件"""
        print("📋 可用数据文件:")
        print("-" * 40)
        
        data_files = []
        for filename in os.listdir(self.data_dir):
            if filename.startswith("all_market_data_") and filename.endswith(".pkl"):
                date_str = filename.replace("all_market_data_", "").replace(".pkl", "")
                file_path = os.path.join(self.data_dir, filename)
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                
                data_files.append((date_str, filename, file_size))
        
        # 按日期排序
        data_files.sort(key=lambda x: x[0], reverse=True)
        
        if data_files:
            for date_str, filename, file_size in data_files:
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                print(f"📅 {formatted_date}: {filename} ({file_size:.1f} MB)")
        else:
            print("暂无可用数据文件")
    
    def clean_old_data(self, keep_days=7):
        """
        清理旧数据
        
        Args:
            keep_days: 保留天数
        """
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        cutoff_str = cutoff_date.strftime("%Y%m%d")
        
        print(f"🧹 清理 {cutoff_str} 之前的数据...")
        
        removed_count = 0
        total_size = 0
        
        # 清理主数据文件
        for filename in os.listdir(self.data_dir):
            if ("all_market_data_" in filename or "stock_list_" in filename) and filename.endswith(".pkl"):
                date_str = filename.split("_")[-1].replace(".pkl", "")
                if len(date_str) == 8 and date_str < cutoff_str:
                    file_path = os.path.join(self.data_dir, filename)
                    file_size = os.path.getsize(file_path) / (1024 * 1024)
                    total_size += file_size
                    os.remove(file_path)
                    print(f"🗑️  删除: {filename} ({file_size:.1f} MB)")
                    removed_count += 1
        
        # 清理个股数据
        stocks_dir = os.path.join(self.data_dir, "stocks")
        if os.path.exists(stocks_dir):
            for filename in os.listdir(stocks_dir):
                if filename.endswith(".pkl"):
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        date_str = parts[-1].replace(".pkl", "")
                        if len(date_str) == 8 and date_str < cutoff_str:
                            file_path = os.path.join(stocks_dir, filename)
                            try:
                                file_size = os.path.getsize(file_path) / (1024 * 1024)
                                total_size += file_size
                                os.remove(file_path)
                                removed_count += 1
                            except:
                                pass
        
        print(f"✅ 清理完成: 删除了 {removed_count} 个文件，释放 {total_size:.1f} MB 空间")
    
    def get_data_statistics(self):
        """获取数据统计信息"""
        print("📊 数据统计信息:")
        print("-" * 40)
        
        # 基本信息
        print(f"📁 数据目录: {self.data_dir}")
        print(f"📅 最后更新: {self.metadata.get('last_update', '未更新')}")
        print(f"📈 股票数量: {self.metadata.get('stock_count', 0)}")
        
        # 数据文件统计
        total_files = 0
        total_size = 0
        
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith('.pkl'):
                    file_path = os.path.join(root, file)
                    try:
                        size = os.path.getsize(file_path) / (1024 * 1024)
                        total_size += size
                        total_files += 1
                    except:
                        pass
        
        print(f"📦 数据文件: {total_files} 个")
        print(f"💾 总大小: {total_size:.1f} MB")
        
        # 更新历史
        update_history = self.metadata.get('update_history', [])
        if update_history:
            print(f"\n📋 最近更新记录:")
            for record in update_history[-5:]:  # 显示最近5次
                date = record['date']
                formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
                success_rate = record['successful'] / record['total_stocks'] * 100
                print(f"   {formatted_date}: {record['successful']}/{record['total_stocks']} "
                      f"({success_rate:.1f}%), 耗时 {record['duration']:.1f}秒")

def main():
    """主函数 - 演示用法"""
    print("🎯 A股全市场数据管理系统")
    print("=" * 60)
    
    # 初始化数据管理器
    manager = StockDataManager()
    
    # 显示当前状态
    manager.get_data_statistics()
    
    # 列出可用数据
    print("\n")
    manager.list_available_data()
    
    # 获取全市场数据（测试模式，只获取前20只股票）
    print(f"\n🚀 开始获取数据...")
    all_data = manager.download_all_market_data(max_stocks=20)
    
    print(f"\n📈 数据获取结果:")
    for i, (code, stock_info) in enumerate(list(all_data.items())[:5]):
        data = stock_info['data']
        info = stock_info['info']
        print(f"   {code} ({info['name']}): {len(data)} 条记录")

if __name__ == "__main__":
    main()
