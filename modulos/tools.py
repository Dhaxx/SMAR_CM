from conexao import cur_fdb, commit

ANO = int(cur_fdb.execute("SELECT mexer FROM cadcli").fetchone()[0])
EMPRESA = cur_fdb.execute("SELECT empresa FROM cadcli").fetchone()[0]

def extourou_codigo_item(grupo, subgrupo):
    nome = cur_fdb.execute(f"select nome from cadsubgr where grupo = {grupo} and subgrupo = {subgrupo}").fetchone()
    return nome

def cria_campo(query):
    try:
        cur_fdb.execute(query)
        commit()
    except:
        pass