import pandas as pd
import glob
import os

# 数据存放路径
SOURCE_DIR = './all_data/'


def combine_all_files(source_dir, output_filename, keyword):
    """
    读取源目录中所有符合关键词的训练TSV文件，合并数据，并保存结果。
    """
    # 查找所有包含关键词的训练TSV文件
    # os.path.join 确保路径正确，*keyword*.tsv 匹配包含关键词的所有tsv文件
    all_train_files = glob.glob(os.path.join(source_dir, f"*{keyword}*.tsv"))

    if not all_train_files:
        print(f"在目录 {source_dir} 中未找到包含关键词 '{keyword}' 的训练文件。请检查路径和关键词。")
        return

    print(f"找到 {len(all_train_files)} 个训练文件，开始合并...")
    
    df_list = []
    
    # 2. 循环读取和记录数据源
    for filename in all_train_files:
        file_name = os.path.basename(filename)
        try:
            # 读取TSV文件
            df = pd.read_csv(filename, sep='\t')
            # 添加源文件信息，方便追溯
            df['Source_File'] = file_name 

            # 抽取数据
            rate = 0.7 if len(df) < 10000 else 0.07

            # 正样本
            df_1 = df[df['y'] == 1].copy()
            num = len(df_1) * rate 
            df_sample_1 = df_1.sample(frac=rate, random_state=42)
            # 负样本
            df_0 = df[df['y'] == 0].copy()
            df_sample_0 = df_0.sample(n=int(num), random_state=42)

            # 使用 frac 参数进行比例抽样
            df_sample = pd.concat([df_sample_0, df_sample_1], axis=0, ignore_index=True)
            df_list.append(df_sample)

            #print(f"  - 成功读取：{file_name}, 行数：{len(df)} ,{df_sample['y'].value_counts()}")
            
        except Exception as e:
            print(f"  - [错误] 读取文件 {file_name} 失败。错误信息：{e}")

    # 3. 合并所有数据
    # Pandas 会自动处理不同文件中的列名不一致问题，缺失值会用 NaN 填充。
    if not df_list:
        print("\n没有成功读取任何数据，无法合并。")
        return
        
    combined_df = pd.concat(df_list, ignore_index=True, sort=False)
    
    print(f"\n所有文件合并完成。总行数：{len(combined_df)}")

    # 4. 保存结果
    try:
        # 保存为新的TSV文件
        # sep='\t' 确保是TSV格式
        # index=False 不保存DataFrame的行索引
        combined_df.to_csv(output_filename, sep='\t', index=False)
        print(f"🎉 最终整合结果已成功保存到文件：{output_filename}")
    except Exception as e:
        print(f"错误：保存文件失败。错误信息：{e}")


# --- 运行主函数 ---
if __name__ == '__main__':
    
    # 创建训练集
    combine_all_files('./all_data/', 'training_data.tsv', 'training')
    # 创建测试集
    combine_all_files(SOURCE_DIR, 'testing_data_70.tsv', 'testing_70')
    combine_all_files(SOURCE_DIR, 'testing_data_80.tsv', 'testing_80')
    combine_all_files(SOURCE_DIR, 'testing_data_90.tsv', 'testing_90')