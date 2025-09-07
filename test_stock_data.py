#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票数据测试脚本
用于诊断数据下载问题
"""

import sys
import os

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_client import StockDataClient
import pandas as pd

def test_stock_data():
    """测试股票数据获取"""
    
    print("🔍 开始股票数据诊断...")
    print("=" * 50)
    
    # 初始化客户端
    client = StockDataClient()
    
    # 1. 测试股票列表获取
    print("\n1️⃣ 测试股票列表获取:")
    stocks = client.get_stock_list()
    print(f"股票列表长度: {len(stocks)}")
    
    if not stocks.empty:
        print("前10只股票:")
        print(stocks.head(10)[['code', 'name']])
        
        print("\n股票代码分布:")
        if 'code' in stocks.columns:
            code_patterns = {
                '6开头(上海)': len(stocks[stocks['code'].str.startswith('6')]),
                '000开头': len(stocks[stocks['code'].str.startswith('000')]),
                '002开头(中小板)': len(stocks[stocks['code'].str.startswith('002')]),
                '300开头(创业板)': len(stocks[stocks['code'].str.startswith('300')])
            }
            for pattern, count in code_patterns.items():
                print(f"  {pattern}: {count} 只")
    
    # 2. 测试真实股票数据下载
    print("\n2️⃣ 测试真实股票数据下载:")
    
    # 测试一些知名的股票代码
    test_codes = [
        ('000401', 0, '冀东水泥'),  # 用户要求测试的股票
        ('000001', 0, '平安银行'),
        ('000002', 0, '万科A'),
        ('600000', 1, '浦发银行'),
        ('600036', 1, '招商银行'),
        ('000858', 0, '五 粮 液'),
        ('002415', 0, '海康威视')
    ]
    
    success_count = 0
    for code, market, name in test_codes:
        print(f"\n测试 {code} ({name}):")
        try:
            data = client.get_daily_data(code, market=market, count=30)
            if not data.empty:
                print(f"  ✅ 成功获取 {len(data)} 条数据")
                print(f"  最新收盘价: {data['收盘'].iloc[-1]:.2f}")
                success_count += 1
            else:
                print(f"  ❌ 数据为空")
        except Exception as e:
            print(f"  ❌ 异常: {e}")
    
    print(f"\n📊 测试结果: {success_count}/{len(test_codes)} 只股票数据获取成功")
    
    # 3. 检查是否为指数数据问题
    print("\n3️⃣ 检查指数数据:")
    index_codes = ['000001', '000002', '000016']  # 上证指数、A股指数、上证50
    
    for code in index_codes:
        print(f"\n测试指数 {code}:")
        try:
            # 尝试作为指数获取
            data = client.get_daily_data(code, market=1, count=30)
            if not data.empty:
                print(f"  ✅ 指数数据成功: {len(data)} 条")
            else:
                print(f"  ❌ 指数数据为空")
        except Exception as e:
            print(f"  ❌ 指数数据异常: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 诊断完成!")

if __name__ == "__main__":
    test_stock_data()
