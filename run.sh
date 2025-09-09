#!/bin/bash
# 
# A股全市场数据管理快速启动脚本
#

echo "🎯 A股全市场数据管理系统"
echo "=========================="

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "❌ 未找到Python3，请先安装Python"
    exit 1
fi

# 切换到脚本所在目录
cd "$(dirname "$0")"

# 显示菜单
echo "请选择操作:"
echo "1. 下载全市场数据 (测试模式 - 100只股票)"
echo "2. 下载全市场数据 (完整模式 - 所有A股股票) [推荐]"
echo "3. 强制更新全市场数据"
echo "4. 查看可用数据"
echo "5. 查看数据统计"
echo "6. 清理旧数据"
echo "7. 运行B1选股策略 (全市场分析) [推荐]"
echo "8. 运行B1选股策略 (测试模式 - 100只股票)"
echo "9. 运行B1选股策略 (先下载全市场数据)"
echo "10. 指定日期选股分析 (例如：2024-03-15)"
echo "11. 指定日期数据下载 (例如：2024-03-15)"
echo "0. 退出"

read -p "请输入选择 [0-11]: " choice

case $choice in
    1)
        echo "📥 开始下载全市场数据 (测试模式)..."
        python3 download_market_data.py --test
        ;;
    2)
        echo "📥 开始下载全市场数据 (完整模式 - 所有A股股票)..."
        echo "使用多线程加速下载..."
        python3 download_market_data.py --all-stocks --max-workers 15 --batch-size 30
        ;;
    3)
        echo "🔄 强制更新全市场数据..."
        echo "使用多线程强制更新..."
        python3 download_market_data.py --force --max-workers 15 --batch-size 30
        ;;
    4)
        echo "📋 查看可用数据..."
        python3 download_market_data.py --list
        ;;
    5)
        echo "📊 查看数据统计..."
        python3 download_market_data.py --stats
        ;;
    6)
        echo "🧹 清理旧数据..."
        python3 download_market_data.py --clean
        ;;
    7)
        echo "🚀 运行B1选股策略 (全市场分析)..."
        python3 main.py --all-stocks
        ;;
    8)
        echo "🚀 运行B1选股策略 (测试模式)..."
        python3 main.py --test-mode
        ;;
    9)
        echo "🚀 运行B1选股策略 (先下载全市场数据)..."
        echo "使用多线程加速下载..."
        python3 main.py --download-first --all-stocks --max-workers 15
        ;;
    10)
        echo "🎯 指定日期选股分析"
        echo "格式示例: 2024-03-15"
        read -p "请输入分析日期 (YYYY-MM-DD): " analysis_date
        if [[ -z "$analysis_date" ]]; then
            echo "❌ 未输入日期，使用当前日期"
            python3 main.py --all-stocks
        else
            echo "🚀 运行B1选股策略 (分析日期: $analysis_date)..."
            python3 main.py --all-stocks --date "$analysis_date"
        fi
        ;;
    11)
        echo "📥 指定日期数据下载"
        echo "格式示例: 2024-03-15"
        read -p "请输入下载结束日期 (YYYY-MM-DD): " download_date
        if [[ -z "$download_date" ]]; then
            echo "❌ 未输入日期，下载到当前日期"
            python3 download_market_data.py --all-stocks --max-workers 15
        else
            echo "📥 开始下载到指定日期的数据 (结束日期: $download_date)..."
            python3 download_market_data.py --all-stocks --max-workers 15 --end-date "$download_date"
        fi
        ;;
    0)
        echo "👋 退出"
        exit 0
        ;;
    *)
        echo "❌ 无效选择"
        exit 1
        ;;
esac

echo ""
echo "操作完成! 按任意键退出..."
read -n 1
