import psycopg2
import pandas as pd
import re
import os
from dotenv import load_dotenv

load_dotenv()

def desctable(cur,table):
    cur.execute(f"SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '{table}';")
    return(cur.fetchall())

def where(cur,table,columns):
    # WHERE id='2';
    ncolumn = input("column: ")
    if ncolumn not in columns:
        print(f"\n!!! coloums name not in table {table} !!!")
        return where(cur,table,columns)
    else:
        # [('id', 'integer'), ('email', 'character varying'), ('name', 'character varying'), ('password', 'character varying')]
        for x in desctable(cur,table):
            if ncolumn == x[0]:
                if x[1] == "integer":
                    brew = int(input(f"{ncolumn}: "))
                else:
                    brew = float(input(f"{ncolumn}: "))
            else:
                brew = input(f"{ncolumn}: ")
            break
    return(f"{ncolumn}='{brew}'")


def change(array):
    return (re.search(r'\[(.*?)\]', str(array)).group(1))

def update(conn,cur,table,columns):
    # UPDATE table_name SET column1 = value1, column2 = value2, WHERE condition;
    print("\n*************************")
    print("*   UPDATE Statement    *")
    thang = int(input("\n* How Many Statement To Update *\n: "))
    dubstep = []
    for x in range(thang):
        selection = where(cur,table,columns)
        dubstep.append(selection)
    result = change(dubstep).replace('"', '')
    print("\n*   WHERE Clause   *")
    resultw = where(cur,table,columns)
    cur.execute(f"UPDATE {table} SET {result} WHERE {resultw}")
    conn.commit()
    print(f"Successfully UPDATE Statement {result}")

def delete(conn,cur,table,columns):
    print("\n*************************")
    print("*   Delete Statement    *")
    print("*  1. VIEW  2. DELETE   *")
    print("*************************")
    option_t = int(input(": "))
    if option_t == 1:
        return select(cur,table,columns)
    elif option_t == 2:
        legend = where(cur,table,columns)
        try:
            cur.execute(f"select * from {table} where {legend}")
            if cur.fetchall() == []:
                 print(f"\n!!! value not in table {table} !!!")
                 return delete(conn,cur,table,columns)
        except Exception as e:
            print(e)
        print("received")
        cur.execute(f"DELETE FROM {table} where {legend}")
        conn.commit()
        print(f"Successfully DELETE Statement ^^")
    else:
        exit(1)

    return

def insert(conn,cur,table,columns):
    print("\n***************")
    print("* Insert Data *")
    print("****************")
    numeric = ["integer","real","smallint","bigint","decimal","numeric","real","double","precision","serial","bigserial"]
    cur.execute(f"SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '{table}';")
    visible = cur.fetchall()
    wonda =[]
    try:
        for x in visible:
            if x[1] in numeric:
                if x[1] == "integer":
                    brew = int(input(f"{x[0]}: "))
                else:
                    brew = float(input(f"{x[0]}: "))
            else:
                brew = input(f"{x[0]}: ")
            wonda.append(brew)
    except Exception as e:
        print("\n!!! Error Input Data Type !!!")
        return insert(conn,cur,table,columns)
    coloums = change(columns).replace("'", "")
    cur.execute(f"INSERT INTO {table} ({coloums}) VALUES ({change(wonda)})")
    conn.commit()
    print("\n***************************")
    print("* Successfully User INSERT *")
    while visible:
        cur.execute(f"SELECT * FROM {table} WHERE {visible[0][0]} ='{wonda[0]}'")
        print([i for sub in cur.fetchall() for i in sub])
        break
    print("****************************")

def select(cur,table,columns):
    print("\n*********************")
    opt = int(input("*  1.Auto 2.Manual(Under Maintenance)  *\n: "))
    if opt == 2:
#        loop = int(input("how many column: "))
#        coloum =[]
#        for x in range(loop):
#            coloum.append(input("column :"))
#        cur.execute(f'SELECT {",".join(coloum)} FROM {table}')
#        print(cur.fetchall())
         return(select(cur,table,columns))
    elif opt ==1:
        cur.execute(f'SELECT * FROM {table}')
        test = cur.fetchall()
        k=[]
        i=0
        for x in test:
            k.append(list(test[i]))
            i+=1
        print("\n*************************************")
        df = pd.DataFrame(k,columns=columns)
        print(df.to_string(index=False))

def main():
    conn = psycopg2.connect(database="{}".format(os.getenv("AKJYI")), user = "{}".format(os.getenv("JAHYL")), password = "{}".format(os.getenv("NMKSU")), host = "{}".format(os.getenv("NJDSY")), port = "{}".format(os.getenv("BGSTB")))
    cur = conn.cursor()

    print("\n\t██████╗  ██████╗ ███████╗████████╗ ██████╗ ██████╗ ███████╗███████╗ ██████╗ ██╗ ")
    print("\t██╔══██╗██╔═══██╗██╔════╝╚══██╔══╝██╔════╝ ██╔══██╗██╔════╝██╔════╝██╔═══██╗██║ ")
    print("\t██████╔╝██║   ██║███████╗   ██║   ██║  ███╗██████╔╝█████╗  ███████╗██║   ██║██║  ")
    print("\t██╔═══╝ ██║   ██║╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  ╚════██║██║▄▄ ██║██║ ")
    print("\t██║     ╚██████╔╝███████║   ██║   ╚██████╔╝██║  ██║███████╗███████║╚██████╔╝███████╗")
    print("\t╚═╝      ╚═════╝ ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝ ╚══▀▀═╝ ╚══════╝")

    print("\n**************")
    print("* List Table *")
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
    ltable = [i for sub in cur.fetchall() for i in sub]
    print(f"{ltable}\n**************")

    print("\n****************")
    print("* Choose Table *")
    print("****************")
    table = input(": ")
    if table not in ltable:
        print("\n!!! Table Name Not In Database !!!")
        return main()

    print("\n****************************")
    print("* Name Coloums in Database *")
    cur.execute(f"SELECT column_name FROM information_schema.columns where table_name = '{table}';")
    columns = [i for sub in cur.fetchall() for i in sub]
    print(columns)
    print("****************************")

    print("\n*********************************************")
    print("*           Choose Number Operation           *")
    print("*  1. INSERT  2. SELECT  3. DELETE 4. UPDATE  *")
    print("***********************************************")
    Nu = int(input(": "))
    if Nu == 1:
        insert(conn,cur,table,columns)
    if Nu == 2:
        select(cur,table,columns)
    if Nu == 3:
        delete(conn,cur,table,columns)
    if Nu == 4:
        update(conn,cur,table,columns)
    exit()

if __name__ == "__main__":
    main()
