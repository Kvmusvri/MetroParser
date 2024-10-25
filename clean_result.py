import pandas as pd

def main():
    msk_table = pd.read_excel('moscow_table.xlsx', engine='openpyxl')
    spb_table = pd.read_excel('spb_table.xlsx', engine='openpyxl')

    msk_table.drop(0, axis=1, inplace=True)
    spb_table.drop(0, axis=1, inplace=True)


    msk_table.dropna(how='all', inplace=True)
    spb_table.dropna(how='all', inplace=True)

    with pd.ExcelWriter('result_table.xlsx') as writer:
        msk_table.to_excel(writer, sheet_name='Moscow', index=False)
        spb_table.to_excel(writer, sheet_name='Saint_Petersburg', index=False)




if __name__=='__main__':
    main()