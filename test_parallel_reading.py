#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试多线程数据读取功能
"""

import sys
import os
import time
from datetime import datetime

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from stock_selector import StockSelector

def test_single_thread_vs_multi_thread():
    """测试单线程 vs 多线程性能对比"""
    print("🧪 多线程数据读取性能测试")
    print("=" * 60)
    
    # 初始化选股器
    selector = StockSelector(data_dir='stock_data')
    
    # 获取前100只股票作为测试样本
    print("📋 获取测试股票列表...")
    all_stocks = selector.data_client.get_stock_list()
    if all_stocks.empty:
        print("❌ 无法获取股票列表")
        return
    
    # 过滤出真正的股票
    real_stocks = all_stocks[
        (~all_stocks['name'].str.contains('指数|指|ETF|LOF|ST|退', na=False)) &
        (all_stocks['code'].str.match(r'^(6\d{5}|000\d{3}|002\d{3}|300\d{3})$', na=False))
    ].head(100)  # 取前100只
    
    print(f"✅ 获取到 {len(real_stocks)} 只测试股票")
    
    # 测试多线程读取 (线程数=10)
    print(f"\n🚀 测试多线程读取 (10线程, 批大小25)...")
    start_time = time.time()
    
    stocks_data_parallel = selector.load_stocks_data_parallel(
        real_stocks,
        analysis_date=None,
        max_workers=10,
        batch_size=25
    )
    
    parallel_time = time.time() - start_time
    parallel_count = len(stocks_data_parallel)
    
    print(f"📊 多线程结果:")
    print(f"   ✅ 成功读取: {parallel_count} 只股票")
    print(f"   ⏱️  总耗时: {parallel_time:.2f} 秒")
    print(f"   📈 平均速度: {parallel_count/parallel_time:.2f} 只/秒")
    
    # 测试单线程读取（使用传统方法）
    print(f"\n🐌 测试单线程读取 (模拟传统方法)...")
    start_time = time.time()
    
    stocks_data_single = {}
    single_count = 0
    max_period = max(selector.m1, selector.m2, selector.m3, selector.m4)
    data_count = max_period + 30
    
    for idx, row in real_stocks.head(50).iterrows():  # 只测试前50只，节省时间
        code = row['code']
        market = 1 if code.startswith('6') else 0
        
        try:
            data = selector.get_stock_data_for_date(code, market, None, data_count)
            if not data.empty:
                data = selector.calculate_technical_indicators(data)
                stocks_data_single[code] = data
                single_count += 1
        except Exception as e:
            print(f"❌ {code} 失败: {e}")
            continue
    
    single_time = time.time() - start_time
    
    print(f"📊 单线程结果 (前50只):")
    print(f"   ✅ 成功读取: {single_count} 只股票")
    print(f"   ⏱️  总耗时: {single_time:.2f} 秒")
    print(f"   📈 平均速度: {single_count/single_time:.2f} 只/秒")
    
    # 性能对比（基于前50只的对比）
    if single_count > 0:
        parallel_speed_50 = 50 / (parallel_time * 50 / parallel_count)  # 估算多线程处理50只的时间
        single_speed = single_count / single_time
        speedup = parallel_speed_50 / single_speed if single_speed > 0 else 0
        
        print(f"\n📈 性能对比:")
        print(f"   多线程速度 (估算): {parallel_speed_50:.2f} 只/秒")
        print(f"   单线程速度: {single_speed:.2f} 只/秒")
        print(f"   🚀 加速比: {speedup:.2f}x")
    
    print(f"\n✅ 性能测试完成!")
    return stocks_data_parallel

def test_different_thread_configs():
    """测试不同线程配置的性能"""
    print("\n🔧 不同线程配置性能测试")
    print("=" * 60)
    
    selector = StockSelector(data_dir='stock_data')
    
    # 获取测试股票
    all_stocks = selector.data_client.get_stock_list()
    if all_stocks.empty:
        print("❌ 无法获取股票列表")
        return
    
    test_stocks = all_stocks[
        (~all_stocks['name'].str.contains('指数|指|ETF|LOF|ST|退', na=False)) &
        (all_stocks['code'].str.match(r'^(6\d{5}|000\d{3}|002\d{3}|300\d{3})$', na=False))
    ].head(50)  # 测试50只股票
    
    # 测试不同配置
    configs = [
        {"max_workers": 5, "batch_size": 25, "name": "5线程-批25"},
        {"max_workers": 10, "batch_size": 25, "name": "10线程-批25"},
        {"max_workers": 10, "batch_size": 50, "name": "10线程-批50"},
        {"max_workers": 15, "batch_size": 25, "name": "15线程-批25"},
    ]
    
    results = []
    
    for config in configs:
        print(f"\n🧪 测试配置: {config['name']}")
        print("-" * 30)
        
        start_time = time.time()
        
        stocks_data = selector.load_stocks_data_parallel(
            test_stocks,
            analysis_date=None,
            max_workers=config['max_workers'],
            batch_size=config['batch_size']
        )
        
        elapsed_time = time.time() - start_time
        success_count = len(stocks_data)
        speed = success_count / elapsed_time if elapsed_time > 0 else 0
        
        results.append({
            "config": config['name'],
            "time": elapsed_time,
            "count": success_count,
            "speed": speed
        })
        
        print(f"   结果: {success_count} 只股票, {elapsed_time:.2f} 秒, {speed:.2f} 只/秒")
    
    # 显示对比结果
    print(f"\n📊 配置对比结果:")
    print("-" * 50)
    for result in sorted(results, key=lambda x: x['speed'], reverse=True):
        print(f"   {result['config']:<12}: {result['speed']:.2f} 只/秒 "
              f"({result['count']} 只, {result['time']:.2f} 秒)")
    
    return results

if __name__ == "__main__":
    print("🎯 多线程数据读取测试工具")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 基本性能测试
        test_single_thread_vs_multi_thread()
        
        # 不同配置测试
        test_different_thread_configs()
        
        print(f"\n🎉 所有测试完成!")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
