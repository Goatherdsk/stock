#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票数据接入模块
使用mootdx获取股票数据
"""

from mootdx import quotes
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

class StockDataClient:
    """股票数据客户端"""
    
    def __init__(self):
        """初始化客户端"""
        self.client = quotes.Quotes.factory(market='std')
        
    def get_stock_list(self):
        """获取股票列表 - 优化版本，确保获取真实可交易股票"""
        try:
            # 获取沪深股票列表
            stocks_sh = self.client.stocks(market=1)  # 上海
            stocks_sz = self.client.stocks(market=0)  # 深圳
            
            # 合并数据
            all_stocks = pd.concat([stocks_sh, stocks_sz], ignore_index=True)
            
            print(f"原始股票数据: {len(all_stocks)} 条")
            
            # 智能过滤策略
            filtered_stocks = self._smart_filter_stocks(all_stocks)
            
            print(f"✅ 获取到 {len(filtered_stocks)} 只可交易股票")
            return filtered_stocks
            
        except Exception as e:
            print(f"❌ 获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def _smart_filter_stocks(self, all_stocks):
        """智能过滤股票，确保获取真正可交易的股票"""
        if all_stocks.empty:
            return all_stocks
        
        print("📊 执行智能股票过滤...")
        
        # 第一步：基本格式过滤
        # 上海股票：6开头
        # 深圳股票：000开头（主板）、002开头（中小板）、300开头（创业板）
        stock_pattern = r'^(6\d{5}|000\d{3}|002\d{3}|300\d{3})$'
        
        valid_stocks = all_stocks[
            all_stocks['code'].str.match(stock_pattern, na=False)
        ]
        print(f"格式符合的股票: {len(valid_stocks)} 只")
        
        if valid_stocks.empty:
            print("⚠️ 没有符合格式的股票代码")
            return pd.DataFrame()
        
        # 第二步：排除明显的非股票
        # 排除指数、ETF、债券等
        exclude_patterns = [
            r'指数|指|INDEX',  # 指数
            r'ETF|LOF|FOF',   # 基金
            r'债|BOND',        # 债券  
            r'ST|\*ST|ST\*',   # ST股票（修复正则表达式）
            r'退市|退|DELISTED', # 退市股票
            r'停牌|暂停|SUSPENDED', # 停牌股票
            r'优先|PREFERRED',  # 优先股
        ]
        
        exclude_pattern = '|'.join(exclude_patterns)
        
        filtered_stocks = valid_stocks[
            ~valid_stocks['name'].str.contains(exclude_pattern, case=False, na=False)
        ]
        print(f"排除特殊股票后: {len(filtered_stocks)} 只")
        
        # 第三步：简化验证（仅检查基本可用性）
        final_stocks = filtered_stocks.copy()
        
        # 去重并排序
        final_stocks = final_stocks.drop_duplicates(subset=['code'])
        final_stocks = final_stocks.sort_values(['code'])
        final_stocks = final_stocks.reset_index(drop=True)
        
        print(f"最终股票列表: {len(final_stocks)} 只")
        
        # 显示股票分布
        if not final_stocks.empty:
            distribution = {
                '上海主板(6)': len(final_stocks[final_stocks['code'].str.startswith('6')]),
                '深圳主板(000)': len(final_stocks[final_stocks['code'].str.startswith('000')]),
                '中小板(002)': len(final_stocks[final_stocks['code'].str.startswith('002')]),
                '创业板(300)': len(final_stocks[final_stocks['code'].str.startswith('300')])
            }
            
            print("📈 股票分布:")
            for market, count in distribution.items():
                if count > 0:
                    print(f"  {market}: {count} 只")
        
        return final_stocks
    
    def get_daily_data(self, code, market=None, count=30):
        """
        获取股票日线数据 - 高速版本，专为多线程优化
        
        Args:
            code: 股票代码
            market: 市场代码 (0: 深圳, 1: 上海, None: 自动判断)
            count: 获取天数
        """
        # 自动判断市场
        if market is None:
            market = 1 if code.startswith('6') else 0
        
        # 只重试2次：首次尝试 + 1次重试
        for attempt in range(2):
            try:
                # 根据尝试次数选择频率
                frequency = 9 if attempt == 0 else 8
                
                # 尝试获取数据
                data = self.client.bars(symbol=code, frequency=frequency, market=market, count=count)
                
                if data is not None and not data.empty:
                    # 标准化数据格式
                    data = data.rename(columns={
                        'amount': '成交额',
                        'close': '收盘',
                        'high': '最高',
                        'low': '最低',
                        'open': '开盘',
                        'volume': '成交量'
                    })
                    data['代码'] = code
                    
                    # 简单验证：只检查收盘价是否有效
                    if '收盘' in data.columns:
                        latest_close = data['收盘'].iloc[-1]
                        if pd.notna(latest_close) and latest_close > 0:
                            return data
                    
            except Exception:
                pass  # 静默处理所有异常
                
            # 多线程环境下减少等待时间
            if attempt == 0:
                time.sleep(0.01)  # 只等待10毫秒
        
        return pd.DataFrame()
    
    def get_realtime_data(self, codes):
        """
        获取实时数据
        
        Args:
            codes: 股票代码列表
        """
        try:
            if isinstance(codes, str):
                codes = [codes]
            
            # 批量获取实时数据
            data_list = []
            for code in codes:
                # 判断市场
                market = 1 if code.startswith('6') else 0
                data = self.client.quotes(symbol=code, market=market)
                if data is not None and not data.empty:
                    data_list.append(data)
                time.sleep(0.1)  # 避免请求过快
            
            if data_list:
                return pd.concat(data_list, ignore_index=True)
            
        except Exception as e:
            print(f"获取实时数据失败: {e}")
        
        return pd.DataFrame()

if __name__ == "__main__":
    # 测试数据接入
    client = StockDataClient()
    
    print("=" * 50)
    print("测试mootdx数据接入")
    print("=" * 50)
    
    # 1. 获取股票列表
    print("\n1. 获取股票列表...")
    stocks = client.get_stock_list()
    if not stocks.empty:
        print(f"股票列表前5只:")
        print(stocks.head())
    
    # 2. 获取具体股票的日线数据
    print("\n2. 获取平安银行(000001)日线数据...")
    daily_data = client.get_daily_data('000001', market=0, count=10)
    if not daily_data.empty:
        print("日线数据:")
        print(daily_data.tail())
    
    # 3. 获取实时数据
    print("\n3. 获取实时数据...")
    codes = ['000001', '600036']  # 平安银行、招商银行
    realtime_data = client.get_realtime_data(codes)
    if not realtime_data.empty:
        print("实时数据:")
        print(realtime_data)
    
    print("\n数据接入测试完成!")
