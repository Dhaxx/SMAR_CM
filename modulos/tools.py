from conexao import cur_fdb, commit

ANO = int(cur_fdb.execute("SELECT mexer FROM cadcli").fetchone()[0])
EMPRESA = cur_fdb.execute("SELECT empresa FROM cadcli").fetchone()[0]

def extourou_codigo_item(grupo, subgrupo):
    return cur_fdb.execute(f"select nome from cadsubgr where grupo = '{grupo}' and subgrupo_ant = '{subgrupo}'").fetchone()[0]

def cria_campo(query):
    try:
        cur_fdb.execute(query)
        commit()
    except:
        pass

def produtos():
    cur_fdb.execute("select cadpro, cod_ant from cadest")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        hash_map[row['cod_ant']] = row['cadpro']

    return hash_map

def codmodalidades():
    cur_fdb.execute("select codmod, siglamod from modlic")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        hash_map[row['siglamod']] = row['codmod']

    return hash_map
