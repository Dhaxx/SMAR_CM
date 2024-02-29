from conexao import cur_fdb, commit, fetchallmap
from tqdm import tqdm

ANO = int(cur_fdb.execute("SELECT mexer FROM cadcli").fetchone()[0])
EMPRESA = cur_fdb.execute("SELECT empresa FROM cadcli").fetchone()[0]


def extourou_codigo_item(grupo, subgrupo):
    return cur_fdb.execute(
            f"select nome from cadsubgr where grupo = '{grupo}' and subgrupo_ant = '{subgrupo}'").fetchone()[0]


def get_fetchone(sql):
    return cur_fdb.execute(sql).fetchone()[0]


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


def licitacoes():
    cur_fdb.execute("select numpro, sigla_ant, ano, registropreco, numlic from cadlic")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        hash_map[(row['numpro'], row['sigla_ant'], row['ano'], row['registropreco'])] = row['numlic']
    return hash_map

def fornecedores():
    cur_fdb.execute('select nome, codif from desfor')

    hash_map_nome = {}
    hash_map_insmf = {}

    for row in cur_fdb.fetchallmap():
        hash_map_nome[int(row['codif'])] = row['nome'][:40]

    cur_fdb.execute('select distinct(insmf), codif from desfor where insmf is not null')

    for row in cur_fdb.fetchallmap():
        hash_map_insmf[row['insmf']] = int(row['codif'])

    return hash_map_nome, hash_map_insmf

def cadastra_fornecedor():
    consulta = fetchallmap(f"""select
                                    distinct *
                                from
                                    (
                                    select
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        cast(codfor as integer) codif,
                                        case
                                            when selecao = 0 then 'D'
                                            else 'A'
                                        end status,
                                        'N' usa_preferencia,
                                        null nome_ant,
                                        null insmf
                                    from
                                        mat.MCT70100
                                union all
                                    select
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        cast(codfor as integer) codif,
                                        'A' status,
                                        'N' usa_preferencia,
                                        null nome_ant,
                                        null insmf
                                    from
                                        mat.mct90400
                                union all
                                    select
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        NULL codif,
                                        'A' status,
                                        CASE
                                            when direitoPreferencia = 1 then 'S'
                                            else 'N'
                                        END usa_preferencia,
                                        substring(razao_social, 0, 40) nome_ant,
                                        SUBSTRING(documento, 0, 19) insmf
                                    from
                                        mat.MCT81800 m
                                    where
                                        documento is not null
                                union ALL
                                    SELECT
                                        cast(convit as integer) numpro,
                                        sigla sigla_ant,
                                        anoc ano,
                                        cast(codfor as integer) codif,
                                        'A' status,
                                        CASE
                                            when direitoPreferencia = 1 then 'S'
                                            else 'N'
                                        END usa_preferencia,
                                        substring(razao_social, 0, 40) nome_ant,
                                        SUBSTRING(documento, 0, 19) insmf
                                    from
                                        mat.MCT81800 m
                                    where
                                        codfor is not null
                                        and codfor <> '') as query
                                where
                                    ano >= {ANO-5}""")
    
    insert = cur_fdb.prep('insert into desfor (codif, nome, insmf) values (?,?,?)')

    for row in tqdm(consulta):
        try:
            codif = row['codif'] if row['codif'] is not None else cur_fdb.execute(f"select codif from desfor where insmf = '{row['insmf']}'").fetchone()[0]
            nome = cur_fdb.execute(f"select nome from desfor where codif = {codif}").fetchone()[0]
        except:
            codif = cur_fdb.execute('select max(codif)+1 from desfor').fetchone()[0] if codif is None else codif
            cur_fdb.execute(insert,(codif, nome, row['insmf']))
            commit()