from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()

def solicitacoes():

    global PRODUTOS
    # Ao executar o codigo inteiro da main sem nenhum comentario das funcoes, é necessario re-popular o dict
    if len(PRODUTOS) == 0:
        PRODUTOS = produtos()

    cur_fdb.execute("delete from icadorc")
    cur_fdb.execute("delete from cadorc")
    print("Inserindo Solicitações...")

    solicitacoes = {}

    consulta = fetchallmap(f"""select
                                    right('00000' + cast(numreq as varchar),
                                    5)+ '/' + SUBSTRING(anoreq, 3, 5) numorc,
                                    right('00000' + cast(numreq as varchar),5) num,
                                    anoreq ano,
                                    datreq dtorc,
                                    motdev descr,
                                    'NORMAL' prioridade,
                                    CASE 
                                        when obs is not null then obs
                                        else motdev
                                    END obs,
                                    'AP' status,
                                    'S' liberado,
                                    idnivel5 codccusto,
                                    'L' liberado_tela,
                                    ROW_NUMBER() over (order by numreq) id_cadorc
                                from
                                    mat.mct63400
                                where
                                    anoreq in ({ANO}, {ANO - 1})""")
    
    insert = cur_fdb.prep("""insert
                                into
                                cadorc (id_cadorc,
                                num,
                                ano,
                                numorc,    
                                dtorc,
                                descr,  
                                prioridade,
                                obs,
                                status,
                                liberado,
                                codccusto,
                                liberado_tela,
                                empresa) values (?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    for row in tqdm(consulta):
        id_cadorc = row['id_cadorc']
        num = row['num']
        ano = row['ano']
        dtorc = row['dtorc']
        descr =   row['descr'][:1024] if row['descr'] else row['descr']
        prioridade = row['prioridade']
        obs = row['obs']
        status = row['status']
        liberado = row['liberado']
        codccusto = row['codccusto']
        liberado_tela = row['liberado_tela']
        numorc = row['numorc']

        cur_fdb.execute(insert,(id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela, EMPRESA))
        solicitacoes[numorc] = id_cadorc
    commit()

    print("Inserindo Itens de Solicitações...")

    insert = cur_fdb.prep('insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc) values (?,?,?,?,?,?,?,?,?)')
    insert_vcadorc = cur_fdb.prep("""insert into vcadorc (numorc, item, codif, vlruni, vlrtot, ganhou, vlrganhou, id_cadorc) values (?,?,?,?,?,?,?,?)""")

    consulta = fetchallmap(f"""SELECT
                                    right('00000' + cast(cabecalho.numreq as varchar),
                                    5)+ '/' + SUBSTRING(cabecalho.anoreq, 3, 2) numorc, 
                                    [item] = item.nuitem,
                                    [Requisitante] = uo.idNivel5,
                                    [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
                                    [Quantidade] = item.quatde,
                                    [Valor Unitário] = item.valite,
                                    [Total item] = item.totite
                                FROM
                                    mat.MCT63400 cabecalho
                                JOIN mat.MCT63500 item ON
                                    item.numreq = cabecalho.numreq
                                    AND item.anoreq = cabecalho.anoreq
                                JOIN mat.unidorcamentariaW uo on
                                    uo.idnivel5 = cabecalho.idnivel5
                                JOIN mat.MXT62300 produto ON
                                    produto.estrut = item.estrut
                                    AND produto.grupo = item.grupo
                                    AND produto.subgrp = item.subgrp
                                    AND produto.itemat = item.itemat
                                    AND produto.digmat = item.digmat
                                where
                                    cabecalho.anoreq in ({ANO}, {ANO - 1})""")
    
    for row in tqdm(consulta):
        numorc = row['numorc']
        item = row['item']    
        codccusto = row['Requisitante']
        cadpro = PRODUTOS[row['cadpro']]
        qtd = float(row['Quantidade'])
        valor = float(row['Valor Unitário'])
        itemorc = row['item']
        itemorc_ag = row['item']
        id_cadorc = solicitacoes[numorc]
        cur_fdb.execute(insert,(numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc))
        # insert_vcadorc = cur_d.prep("""insert into vcadorc (numorc, item, codif, vlruni, vlrtot, ganhou, vlrganhou, id_cadorc) values (?,?,?,?,?,?,?,?)""")
    commit()