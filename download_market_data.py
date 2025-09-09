#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A股全市场数据下载脚本
"""

import sys
import os
import argparse
from datetime import datetime

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_manager import StockDataManager

def main():
    parser = argparse.ArgumentParser(description='A股全市场数据下载工具')
    parser.add_argument('--force', action='store_true', help='强制更新数据')
    parser.add_argument('--max-stocks', type=int, help='最大股票数量（测试用，不指定则下载所有股票）')
    parser.add_argument('--batch-size', type=int, default=50, help='批处理大小')
    parser.add_argument('--max-workers', type=int, default=10, help='最大线程数（默认10）')
    parser.add_argument('--clean', action='store_true', help='清理旧数据')
    parser.add_argument('--list', action='store_true', help='列出可用数据')
    parser.add_argument('--stats', action='store_true', help='显示数据统计')
    parser.add_argument('--data-dir', type=str, default='stock_data', help='数据存储目录')
    parser.add_argument('--test', action='store_true', help='测试模式：仅下载前100只股票')
    parser.add_argument('--all-stocks', action='store_true', default=True, help='下载所有A股股票（默认开启）')
    parser.add_argument('--end-date', type=str, help='下载数据的结束日期（格式：YYYY-MM-DD），例如：2024-03-15')
    
    args = parser.parse_args()
    
    # 验证并解析结束日期
    end_date = None
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
            print(f"🎯 指定下载结束日期: {end_date.strftime('%Y年%m月%d日')}")
        except ValueError:
            print(f"❌ 日期格式错误: {args.end_date}")
            print("正确格式: YYYY-MM-DD，例如：2024-03-15")
            return 1
    else:
        end_date = datetime.now()
        print(f"🎯 默认下载结束日期: {end_date.strftime('%Y年%m月%d日')} (今天)")
    
    print("🎯 A股全市场数据下载工具")
    print("=" * 60)
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据下载到: {end_date.strftime('%Y年%m月%d日')}")
    
    # 初始化数据管理器
    try:
        manager = StockDataManager(data_dir=args.data_dir)
    except Exception as e:
        print(f"❌ 数据管理器初始化失败: {e}")
        return 1
    
    try:
        if args.stats:
            # 显示数据统计
            manager.get_data_statistics()
            return 0
        
        if args.list:
            # 列出可用数据
            manager.list_available_data()
            return 0
        
        if args.clean:
            # 清理旧数据
            manager.clean_old_data()
            return 0
        
        # 下载全市场数据
        print(f"\n🚀 开始下载全市场数据...")
        
        # 处理不同模式参数
        if args.test:
            max_stocks = 100
            print(f"🧪 测试模式: 限制 {max_stocks} 只股票")
        elif args.max_stocks:
            max_stocks = args.max_stocks
            print(f"🔢 限制模式: 限制 {max_stocks} 只股票")
        elif args.all_stocks:
            max_stocks = None
            print(f"🌍 全市场模式: 下载所有A股股票")
        else:
            # 默认也是全市场模式
            max_stocks = None
            print(f"📊 默认模式: 下载所有A股股票")
        
        if args.force:
            print(f"🔄 强制更新模式")
        
        print(f"🔧 线程配置: 批处理大小={args.batch_size}, 最大线程数={args.max_workers}")
        
        all_data = manager.download_all_market_data(
            force_update=args.force,
            max_stocks=max_stocks,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            end_date=end_date.strftime('%Y%m%d')  # 添加结束日期参数
        )
        
        if all_data:
            print(f"\n📊 下载完成统计:")
            print(f"   获取股票数: {len(all_data)}")
            print(f"   存储位置: {manager.data_dir}")
            
            # 显示部分数据示例
            print(f"\n📈 数据示例:")
            for i, (code, stock_info) in enumerate(list(all_data.items())[:5]):
                data = stock_info['data']
                info = stock_info['info']
                latest = data.iloc[-1] if not data.empty else None
                
                if latest is not None:
                    print(f"   {code} ({info['name']}): "
                          f"最新价格 {latest.get('收盘', 'N/A')}, "
                          f"数据量 {len(data)} 条")
            
            if len(all_data) > 5:
                print(f"   ... 还有 {len(all_data) - 5} 只股票")
        
        print(f"\n🎉 数据下载完成!")
        print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return 0
        
    except KeyboardInterrupt:
        print(f"\n⏹️  用户中断下载")
        return 1
    except Exception as e:
        print(f"\n❌ 下载失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
