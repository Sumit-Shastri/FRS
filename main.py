import pandas
import glob

model_collateral_df = pandas.read_csv(r"E:\lending-club-data\model_collateral.csv")
#print(model_collateral)

model_config_df = pandas.read_csv(r"E:\lending-club-data\model_config.csv")
#print(model_config)

csv_files = glob.glob(r"E:\lending-club-data\model_auth_Rep/*.csv")
combined_model_auth_rep = pandas.DataFrame()
for csv_file in csv_files:
                          df = pandas.read_csv(csv_file)
                          combined_model_auth_rep = pandas.concat([combined_model_auth_rep,df])
print(combined_model_auth_rep)

