#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票B1策略选股系统 - 主程序
支持本地数据和在线数据
"""

import sys
import os
import argparse
from datetime import datetime

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from stock_selector import StockSelector
from data_manager import StockDataManager

def main():
    parser = argparse.ArgumentParser(description='B1股票选股系统')
    parser.add_argument('--use-local', action='store_true', help='使用本地数据')
    parser.add_argument('--download-first', action='store_true', default=True, help='先下载最新数据再选股(默认开启)')
    parser.add_argument('--stock-count', type=int, help='分析股票数量(不指定则分析所有A股)')
    parser.add_argument('--test-mode', action='store_true', help='测试模式：只分析前100只股票')
    parser.add_argument('--all-stocks', action='store_true', default=True, help='分析所有A股股票(默认开启)')
    parser.add_argument('--max-workers', type=int, default=10, help='下载数据时的最大线程数（默认10）')
    parser.add_argument('--m1', type=int, default=14, help='知行多空线参数M1')
    parser.add_argument('--m2', type=int, default=28, help='知行多空线参数M2')
    parser.add_argument('--m3', type=int, default=57, help='知行多空线参数M3')
    parser.add_argument('--m4', type=int, default=114, help='知行多空线参数M4')
    parser.add_argument('--data-dir', type=str, default='stock_data', help='数据存储目录')
    parser.add_argument('--stocks', type=str, nargs='+', help='指定分析的股票代码（可以指定多个，空格分隔）')
    parser.add_argument('--date', type=str, help='指定分析日期（格式：YYYY-MM-DD），例如：2024-03-15')
    
    args = parser.parse_args()
    
    # 验证并解析指定日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d')
            print(f"🎯 指定分析日期: {target_date.strftime('%Y年%m月%d日')}")
        except ValueError:
            print(f"❌ 日期格式错误: {args.date}")
            print("正确格式: YYYY-MM-DD，例如：2024-03-15")
            return
    else:
        target_date = datetime.now()
        print(f"🎯 默认分析日期: {target_date.strftime('%Y年%m月%d日')} (今天)")
    
    # 确定分析股票数量和范围
    if args.stocks:
        stock_count = None  # 指定股票时不限制数量
        stock_list = args.stocks
        print(f"🎯 指定股票模式：分析 {', '.join(stock_list)} 共{len(stock_list)}只股票")
    elif args.test_mode:
        stock_count = 100
        stock_list = None
        print("🧪 测试模式：分析前100只股票")
    elif args.stock_count:
        stock_count = args.stock_count
        stock_list = None
        print(f"📊 指定模式：分析前{stock_count}只股票")
    else:
        stock_count = None  # 分析所有股票
        stock_list = None
        print("🌍 全市场模式：分析所有A股股票")
    
    print("🎯 B1股票选股系统启动")
    print("=" * 60)
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"分析日期: {target_date.strftime('%Y年%m月%d日')}")
    
    # 如果需要先下载数据（默认开启）
    if args.download_first and not args.use_local:
        print(f"\n📥 正在下载到 {target_date.strftime('%Y-%m-%d')} 的市场数据...")
        print(f"🔧 使用 {args.max_workers} 个线程并发下载")
        try:
            manager = StockDataManager(data_dir=args.data_dir)
            download_count = stock_count * 2 if stock_count else None  # 如果指定了数量，多下载一些作为备选
            manager.download_all_market_data(
                max_stocks=download_count, 
                max_workers=args.max_workers,
                end_date=target_date.strftime('%Y%m%d')  # 下载到指定日期
            )
        except Exception as e:
            print(f"❌ 数据下载失败: {e}")
            print("继续使用在线数据模式...")
    
    # 初始化选股器
    try:
        selector = StockSelector(m1=args.m1, m2=args.m2, m3=args.m3, m4=args.m4, data_dir=args.data_dir)
        print(f"✅ 选股系统初始化成功")
        print(f"📊 知行多空线参数: M1={args.m1}, M2={args.m2}, M3={args.m3}, M4={args.m4}")
        print(f"💾 数据模式: {'本地数据' if args.use_local else '在线数据'}")
        print(f"📅 分析基准日期: {target_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"❌ 选股系统初始化失败: {e}")
        return
    
    # B1策略选股
    print(f"\n🚀 执行B1策略选股 (基于 {target_date.strftime('%Y-%m-%d')} 数据)...")
    print("-" * 40)
    
    try:
        b1_stocks = selector.run_stock_selection(
            strategy='b1', 
            stock_count=stock_count,
            stock_list=stock_list,
            save_blk=True,
            analysis_date=target_date.strftime('%Y%m%d')  # 传递分析日期
        )
        
        if not b1_stocks.empty:
            print(f"\n✅ B1策略选出 {len(b1_stocks)} 只股票:")
            print("\n📈 B1策略选股结果:")
            print("-" * 80)
            
            # 显示选股结果
            display_columns = ['code', 'name', 'price', 'change_pct', 'j_value', 'range_pct']
            if all(col in b1_stocks.columns for col in display_columns):
                print(b1_stocks[display_columns].to_string(index=False))
            else:
                print(b1_stocks.to_string(index=False))
            
            print(f"\n💾 已生成B1.blk文件，包含 {len(b1_stocks)} 只股票")
            print("📁 文件位置: output/B1_*.blk")
        else:
            print("❌ B1策略未选出符合条件的股票")
    except Exception as e:
        print(f"❌ B1策略执行失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("🎉 B1股票选股系统运行完成!")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("💡 提示: B1.blk文件可直接导入通达信软件使用")
    print("="*60)

if __name__ == "__main__":
    main()