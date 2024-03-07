from conexao import cur_fdb, commit, fetchallmap
from tqdm import tqdm
import re

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

def cotacoes():
    cur_fdb.execute("select numorc, ano, obs, registropreco from cadorc where obs starting 'Agrupamento'")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        agrupamento = re.search(r'\b(\d+)\b',row['obs'])
        hash_map[(agrupamento.group(1),row['ano'],row['registropreco'])] = row['numorc']
    return hash_map

def licitacoes():
    cria_campo('ALTER TABLE CADLIC ADD criterio_ant varchar(30)')
    cria_campo('ALTER TABLE CADLIC ADD sigla_ant varchar(2)')
    cria_campo('ALTER TABLE CADLIC ADD status_ant varchar(1)')

    cur_fdb.execute("select numpro, sigla_ant, ano, registropreco, numlic from cadlic")

    hash_map = {}

    for row in cur_fdb.fetchallmap():
        hash_map[(row['numpro'], row['sigla_ant'], row['ano'])] = row['numlic'] # row['registropreco']
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

def cadastra_fornecedor_geralizado():
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

def cadastra_fornecedor_especifico(insmf):
    insert = cur_fdb.prep('insert into desfor (codif, nome, insmf) values (?,?,?)')

    verifica = cur_fdb.execute(f"select codif from desfor where insmf containing '{insmf}'").fetchone()

    if not verifica:
        codif = cur_fdb.execute('select max(codif)+1 from desfor').fetchone()[0]
        nome = 'Verificar Fornecedor {}'.format(insmf)
        cur_fdb.execute(insert,(codif, nome, insmf))
        commit()
        
        return codif
    
def ajustar_ccusto_cotacao():
    print('Ajustando Centro de custos da cotação...')
    hash_map = {}
    update_cadorc = cur_fdb.prep('update cadorc set codccusto = (select codccusto from centrocusto where cod_ant = ?) where codccusto = ?')
    update_icadorc = cur_fdb.prep('update icadorc set codccusto = (select codccusto from centrocusto where cod_ant = ?) where codccusto = ?')

    consulta = fetchallmap("""select IdNivel5, nivel1 + '.' + nivel2 + '.' + nivel3 +  + '.' + nivel4 + '.' + nivel5 as codant from mat.MXT71100 m""")
    for row in consulta:
        hash_map[row['IdNivel5']] = row['codant']

    cur_fdb.execute('select distinct codccusto from cadorc where codccusto <> 0')
    for row in tqdm(cur_fdb.fetchallmap()):
        cod_ant = hash_map[row['codccusto']]
        cur_fdb.execute(update_cadorc, (cod_ant,row['codccusto']))
        cur_fdb.execute(update_icadorc, (cod_ant,row['codccusto']))
    commit()

def depara_ccusto():
    hash_map = {}
    cur_fdb.execute('select cod_ant, codccusto from centrocusto')

    for row in cur_fdb.fetchallmap():
        hash_map[row['cod_ant']] = row['codccusto']
    return hash_map