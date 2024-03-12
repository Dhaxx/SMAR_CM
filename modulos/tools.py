from conexao import cur_fdb, commit, fetchallmap
from tqdm import tqdm
import re

ANO = int(cur_fdb.execute("SELECT mexer FROM cadcli").fetchone()[0])
EMPRESA = cur_fdb.execute("SELECT empresa FROM cadcli").fetchone()[0]

def fornecedores_smar():
    hash_map = {}

    consulta = fetchallmap("select distinct rtrim(documento) documento, razao_social from mat.MCT81800")

    for row in consulta:
        hash_map[row['documento']] = row['razao_social']
    return hash_map

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
    cria_campo('ALTER TABLE cadest ADD cod_ant varchar(14)')
    hash_map = {}

    cur_fdb.execute("select cadpro, COD_ANT from cadest")

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

def cadastra_fornecedor_especifico(insmf):
    insert = cur_fdb.prep('insert into desfor (codif, nome, insmf) values (?,?,?)')

    verifica = cur_fdb.execute(f"select codif, nome from desfor where insmf containing '{insmf}'").fetchone()

    if not verifica:
        codif = cur_fdb.execute('select max(codif)+1 from desfor').fetchone()[0]
        nome = FORNECEDORES_SMAR.get(insmf,'Verificar Fornecedor {}'.format(insmf)) #'Verificar Fornecedor {}'.format(insmf)
        cur_fdb.execute(insert,(codif, nome[:50], insmf))
        commit()
        return codif
    else:
        return verifica[0]
    
def cadastro_fornecedores_faltantes():
    consulta = fetchallmap(f"""select distinct rtrim(isnull(documento,0)) insmf, substring(razao_social,1,18) nome from mat.MCT81800
                                union all
                                select DISTINCT isnull(insmf,0), 'VER CADASTRO -'+isnull(insmf,0) from (select
                                                                    1 sessao,
                                                                    codfor codif,
                                                                    --ROW_NUMBER() over (partition by isnull(codfor,insmf), convit order by nuitem) item,
                                                                    nuitem itemp,
                                                                    coalesce(qtde, 0) quan1,
                                                                    coalesce(preco, 0) vaun1,
                                                                    coalesce(total, 0) vato1,
                                                                    case when venc is null then 'D' else 'C' end as status,
                                                                    venc subem,
                                                                    rtrim(marca) marca,
                                                                    insmf,
                                                                    right('00000000'+cast(nrolote as varchar),8) lotelic,
                                                                    sigla sigla_ant,
                                                                    convit numpro,
                                                                    anoc ano,
                                                                    'N' registropreco
                                                                from
                                                                    (
                                                                    SELECT 
                                                                        c697.IdProcCompra,
                                                                        c697.unges,
                                                                        c697.sigla,
                                                                        c697.convit,
                                                                        c697.anoc,
                                                                        c803.idlote,
                                                                        nrolote = CASE
                                                                            WHEN c803.idLote IS NULL THEN c698.nuitem
                                                                            ELSE c934.NroLote
                                                                        END,
                                                                        descricao = CASE
                                                                            WHEN c803.idLote IS NULL THEN 'Lote ' + RTRIM(c698.nuitem)
                                                                            ELSE c934.Descricao
                                                                        END,
                                                                        estrut = CASE
                                                                            WHEN c812.estrut_atu IS NULL THEN c698.estrut
                                                                            ELSE c812.estrut_atu
                                                                        END,
                                                                        grupo = CASE
                                                                            WHEN c812.grupo_atu IS NULL THEN c698.grupo
                                                                            ELSE c812.grupo_atu
                                                                        END,
                                                                        subgrp = CASE
                                                                            WHEN c812.subgrp_atu IS NULL THEN c698.subgrp
                                                                            ELSE c812.subgrp_atu
                                                                        END,
                                                                        itemat = CASE
                                                                            WHEN c812.itemat_atu IS NULL THEN c698.itemat
                                                                            ELSE c812.itemat_atu
                                                                        END,
                                                                        digmat = CASE
                                                                            WHEN c812.digmat_atu IS NULL THEN c698.digmat
                                                                            ELSE c812.digmat_atu
                                                                        END,
                                                                        codfor = c698.codfor,
                                                                        c698.codfor_representante,
                                                                        c698.venc,
                                                                        c698.empate,
                                                                        c698.preco,
                                                                        c698.marca,
                                                                        c698.valid,
                                                                        c698.prazo,
                                                                        c698.pgto,
                                                                        c698.nuitem,
                                                                        c698.garantia,
                                                                        qtde = SUM(C803.quantid),
                                                                        total = SUM(ROUND(C803.quantid * c698.preco, 2)),
                                                                        insmf = c072.nrcpfcnpj
                                                                    FROM
                                                                        mat.MCT69700 c697
                                                                    INNER JOIN mat.MCT69800 c698 ON
                                                                        c698.IdProcCompra = c697.IdProcCompra
                                                                    INNER JOIN mat.MCT80200 c802 ON
                                                                        C802.convit = c697.convit
                                                                        AND C802.sigla = c697.sigla
                                                                        AND C802.anoc = c697.anoc
                                                                        AND C802.unges = c697.unges
                                                                        AND c802.aditivo = 0
                                                                    INNER JOIN mat.MCT80300 c803 ON
                                                                        C803.codgrupo = C802.codgrupo
                                                                        AND C803.anogrupo = C802.anogrupo
                                                                        AND C803.unges = C802.unges
                                                                        AND C803.estrut = c698.estrut
                                                                        AND C803.grupo = c698.grupo
                                                                        AND C803.subgrp = c698.subgrp
                                                                        AND C803.itemat = c698.itemat
                                                                        AND C803.digmat = c698.digmat
                                                                        AND ISNULL(c803.idLote,
                                                                        0) = ISNULL(c698.idLote,
                                                                        0)
                                                                    LEFT JOIN mat.MCT81200 c812 ON
                                                                        c812.unges = c697.unges
                                                                        AND c812.sigla = c697.sigla
                                                                        AND c812.anoc = c697.anoc
                                                                        AND c812.convit = c697.convit
                                                                        AND c812.codfor = c698.codfor
                                                                        AND c812.estrut_ant = c698.estrut
                                                                        AND c812.grupo_ant = c698.grupo
                                                                        AND c812.subgrp_ant = c698.subgrp
                                                                        AND c812.itemat_ant = c698.itemat
                                                                        AND c812.digmat_ant = c698.digmat
                                                                    LEFT JOIN mat.MCT93400 c934 ON
                                                                        c934.IdLote = c803.idLote
                                                                    LEFT JOIN mat.MCT07200 c072 ON
                                                                        c072.idfornecedor = c698.idMCT072
                                                                    where c697.anoc >= {ANO-5}
                                                                    GROUP BY
                                                                        c697.IdProcCompra,
                                                                        c697.unges,
                                                                        c697.sigla,
                                                                        c697.convit,
                                                                        c697.anoc,
                                                                        c812.estrut_atu,
                                                                        c812.grupo_atu,
                                                                        c812.subgrp_atu,
                                                                        c812.itemat_atu,
                                                                        c812.digmat_atu,
                                                                        c698.estrut,
                                                                        c698.grupo,
                                                                        c698.subgrp,
                                                                        c698.itemat,
                                                                        c698.digmat,
                                                                        c698.codfor,
                                                                        c698.venc,
                                                                        c698.empate,
                                                                        c698.preco,
                                                                        c698.marca,
                                                                        c698.valid,
                                                                        c698.prazo,
                                                                        c698.pgto,
                                                                        c698.nuitem,
                                                                        c698.garantia,
                                                                        c803.idlote,
                                                                        c934.nrolote,
                                                                        c934.descricao,
                                                                        c698.codfor_representante,
                                                                        c072.nrcpfcnpj) as query
                                                                Union all
                                                                select
                                                                    1 sessao,
                                                                    codfor codif,
                                                                    --ROW_NUMBER() over (partition by codfor, convit order by nuitem)itemp,
                                                                    nuitem,
                                                                    coalesce(qtde,0) quan1,
                                                                    coalesce(preco,0) vaun1,
                                                                    coalesce(total,0) vato1,
                                                                    case when isnull(class,venc) is null then 'D' else 'C' end as status,
                                                                    venc subem,
                                                                    rtrim(marca) marca,
                                                                    insmf,
                                                                    right('00000000'+cast(nrolote as varchar),8) lotelic,
                                                                    sigla sigla_ant,
                                                                    convit numpro,
                                                                    anoc ano,
                                                                    'S' registropreco
                                                                from
                                                                    (
                                                                    SELECT
                                                                        c905.unges,
                                                                        c905.sigla,
                                                                        c905.convit,
                                                                        c905.anoc,
                                                                        c913.idLote,
                                                                        nrolote = CASE
                                                                            WHEN c913.idLote IS NULL THEN c905.nuitem
                                                                            ELSE c934.NroLote
                                                                        END,
                                                                        descricao = CASE
                                                                            WHEN c913.idLote IS NULL THEN 'Lote ' + RTRIM(c905.nuitem)
                                                                            ELSE c934.Descricao
                                                                        END,
                                                                        estrut = CASE
                                                                            WHEN c812.estrut_atu IS NULL THEN c905.estrut
                                                                            ELSE c812.estrut_atu
                                                                        END,
                                                                        grupo = CASE
                                                                            WHEN c812.grupo_atu IS NULL THEN c905.grupo
                                                                            ELSE c812.grupo_atu
                                                                        END,
                                                                        subgrp = CASE
                                                                            WHEN c812.subgrp_atu IS NULL THEN c905.subgrp
                                                                            ELSE c812.subgrp_atu
                                                                        END,
                                                                        itemat = CASE
                                                                            WHEN c812.itemat_atu IS NULL THEN c905.itemat
                                                                            ELSE c812.itemat_atu
                                                                        END,
                                                                        digmat = CASE
                                                                            WHEN c812.digmat_atu IS NULL THEN c905.digmat
                                                                            ELSE c812.digmat_atu
                                                                        END,
                                                                        c905.codfor,
                                                                        c905.venc,
                                                                        c905.class,
                                                                        preco = c905.pr_unit,
                                                                        c905.marca,
                                                                        c905.modelo,
                                                                        c905.nuitem,
                                                                        qtde = SUM(c913.quantid),
                                                                        total = SUM(ROUND(c905.qtde * c905.pr_unit, 2)),
                                                                        insmf = c072.nrcpfcnpj
                                                                    FROM
                                                                        mat.MCT90500 c905
                                                                    INNER JOIN mat.MCT91200 c912 ON
                                                                        c912.unges = c905.unges
                                                                        AND c912.sigla = c905.sigla
                                                                        AND c912.convit = c905.convit
                                                                        AND c912.anoc = c905.anoc
                                                                    INNER JOIN mat.MCT91300 c913 ON
                                                                        c913.unges = c912.unges
                                                                        AND c913.codgrupo = c912.codgrupo
                                                                        AND c913.anogrupo = c912.anogrupo
                                                                        AND c913.estrut = c905.estrut
                                                                        AND c913.grupo = c905.grupo
                                                                        AND c913.subgrp = c905.subgrp
                                                                        AND c913.itemat = c905.itemat
                                                                        AND c913.digmat = c905.digmat
                                                                        AND ISNULL(c913.idLote,
                                                                        0) = ISNULL(c905.idLote,
                                                                        0)
                                                                    LEFT JOIN mat.MCT81200 c812 ON
                                                                        c812.unges = c905.unges
                                                                        AND c812.sigla = c905.sigla
                                                                        AND c812.anoc = c905.anoc
                                                                        AND c812.convit = c905.convit
                                                                        AND c812.codfor = c905.codfor
                                                                        AND c812.estrut_ant = c905.estrut
                                                                        AND c812.grupo_ant = c905.grupo
                                                                        AND c812.subgrp_ant = c905.subgrp
                                                                        AND c812.itemat_ant = c905.itemat
                                                                        AND c812.digmat_ant = c905.digmat
                                                                    LEFT JOIN mat.MCT93400 c934 ON
                                                                        c934.IdLote = c913.idLote
                                                                    LEFT JOIN mat.MCT07200 c072 ON
                                                                        c072.idfornecedor = c905.idMCT072
                                                                    where c905.anoc >= {ANO-5}
                                                                    GROUP BY
                                                                        c905.unges,
                                                                        c905.sigla,
                                                                        c905.convit,
                                                                        c905.anoc,
                                                                        c913.idLote,
                                                                        c812.estrut_atu,
                                                                        c812.grupo_atu,
                                                                        c812.subgrp_atu,
                                                                        c812.itemat_atu,
                                                                        c812.digmat_atu,
                                                                        c905.estrut,
                                                                        c905.grupo,
                                                                        c905.subgrp,
                                                                        c905.itemat,
                                                                        c905.digmat,
                                                                        c905.pr_unit,
                                                                        c913.idLote,
                                                                        c905.codfor,
                                                                        c905.venc,
                                                                        c905.class,
                                                                        c905.nuitem,
                                                                        c905.marca,
                                                                        c905.modelo,
                                                                        c934.nrolote,
                                                                        c934.descricao,
                                                                        c072.nrcpfcnpj) as query) as rn
                                where [insmf] is not null""")
    insert = cur_fdb.prep('insert into desfor (codif, nome, insmf) values (?,?,?)')
    codif = cur_fdb.execute('select max(codif) from desfor').fetchone()[0]

    for row in tqdm(consulta, desc='Inserindo Fornecedores Faltantes'):
        verifica = cur_fdb.execute(f"select nome from desfor where insmf containing '{row['insmf']}'").fetchone()
        if not verifica:
            codif += 1
            nome = row['nome']
            cur_fdb.execute(insert, (codif, nome, row['insmf']))
            commit()
    fornecedores_smar()
FORNECEDORES_SMAR = cadastro_fornecedores_faltantes()
    
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
    cria_campo("alter table centrocusto add cod_ant varchar(19)")
    hash_map = {}
    cur_fdb.execute('select cod_ant, codccusto from centrocusto')

    for row in cur_fdb.fetchallmap():
        hash_map[row['cod_ant']] = row['codccusto']
    return hash_map

def item_da_proposta():
    cria_campo("alter table cadpro_final add CQTDADT double precision")
    cria_campo("alter table cadpro_final add ccadpro varchar(20)")
    cria_campo("alter table cadpro_final add CCODCCUSTO integer;")
    hash_map = {}

    cur_fdb.execute('select numlic, ccadpro, codif, itemp from cadpro_final')

    for row in cur_fdb.fetchallmap():
        hash_map[(row['numlic'], row['ccadpro'], row['codif'])] = row['itemp']
    return hash_map

def veiculo_tipo():
    hash_map = {}

    cur_fdb.execute('select codigo_tip, descricao_tip from veiculo_tipo')

    for row in cur_fdb.fetchallmap():
        hash_map[row['descricao_tip']] = row['codigo_tip']
    return hash_map

def veiculo_marca():
    hash_map = {}

    cur_fdb.execute('SELECT CODIGO_MAR, DESCRICAO_MAR FROM VEICULO_MARCA vm')

    for row in cur_fdb.fetchallmap():
        hash_map[row['descricao_mar']] = row['codigo_mar']
    return hash_map