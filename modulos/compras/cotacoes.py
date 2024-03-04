# I am Satoshi

from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()


def cadastro():
    cur_fdb.execute('Delete from icadorc')
    cur_fdb.execute('Delete from cadorc')
    print("Inserindo Cadastro de Cotações...")

    cria_campo('ALTER TABLE CADORC ADD idant integer')

    global PRODUTOS

    if len(PRODUTOS) == 0:
        PRODUTOS = produtos()
    insert_cadorc = cur_fdb.prep("""insert
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
                                empresa,
                                registropreco, 
                                idant) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

    insert_icadorc = cur_fdb.prep(
        'insert into icadorc (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc) values (?,'
        '?,?,?,?,?,?,?,?)')

    consulta = fetchallmap("""
    select 
        cabecalho.idcotacao numero,
        year(cabecalho.data_cotacao) ano,
        data_cotacao dtorc,
        cabecalho.obs descr,
        'NORMAL' prioridade,
        case when cabecalho.codgrupo  is null then
        'Processo de Compra: ' +  cast(cabecalho.numreq as varchar) + '/' + cast(cabecalho.anoreq as varchar)
        else 'Agrupamento: ' +  cast(cabecalho.codgrupo  as varchar) + '/' + cast(cabecalho.anogrupo as varchar) end obs,
        'AP' status,
        'S' liberado,
        coalesce(cabreq.Idnivel5,0) codccusto,
        'L' liberado_tela,
        case cabecalho.origem when 'R' then 'S' else 'N' end registropreco,
                [item] = ROW_NUMBER() over(PARTITION by cabecalho.idcotacao order by cabecalho.idcotacao , itemgr.nuitem),
                [itemorc_ag] = itemgr.nuitem,
                [Requisitante] = coalesce(cabreq.Idnivel5,0) ,
                [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
                [Quantidade] = coalesce(itemgr.quantid, item.qtde)  ,
                [Valor Unitário] = coalesce(itemgr.vlunit,0) ,
                [Total item] = coalesce(itemgr.vlunit  * itemgr.quantid,0) 
        from mat.MCT79900 cabecalho
        join mat.MCT80000 item on item.idcotacao = cabecalho.idcotacao
                                        JOIN mat.MXT62300 produto ON
                                            produto.estrut = item.estrut
                                            AND produto.grupo = item.grupo
                                            AND produto.subgrp = item.subgrp
                                            AND produto.itemat = item.itemat
                                            AND produto.digmat = item.digmat
        left join mat.MCT80300 itemgr on 
        itemgr.codgrupo = cabecalho.codgrupo 
        and itemgr.anogrupo  = cabecalho.anogrupo 
        and itemgr.unges = cabecalho.unges 
        and itemgr.estrut  = item.estrut 
        and itemgr.grupo  = item.grupo
        and itemgr.subgrp  = item.subgrp
        and itemgr.itemat = item.itemat
        and itemgr.digmat = item.digmat
        left join mat.MCT63400 cabreq on cabreq.unges  = itemgr.unges  and cabreq.anoreq  = itemgr.anoreq and cabreq.numreq  = itemgr.numreq                                  
        where year(data_cotacao) >= %d and cabecalho.origem = 'C' and cabecalho.codgrupo is not null
        union all
        select 
        cabecalho.idcotacao numero,
        year(cabecalho.data_cotacao) ano,
        data_cotacao dtorc,
        cabecalho.obs descr,
        'NORMAL' prioridade,
        case when cabecalho.codgrupo  is null then
        'Processo de Compra: ' +  cast(cabecalho.numreq as varchar) + '/' + cast(cabecalho.anoreq as varchar)
        else 'Agrupamento: ' +  cast(cabecalho.codgrupo  as varchar) + '/' + cast(cabecalho.anogrupo as varchar) end obs,
        'AP' status,
        'S' liberado,
        coalesce(cabreq.Idnivel5,0) codccusto,
        'L' liberado_tela,
        case cabecalho.origem when 'R' then 'S' else 'N' end registropreco,
                [item] = ROW_NUMBER() over(PARTITION by cabecalho.idcotacao order by cabecalho.idcotacao , item.nuitem),
                [itemorc_ag] = item.nuitem,
                [Requisitante] = coalesce(cabreq.Idnivel5,0) ,
                [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
                [Quantidade] = item.qtde  ,
                [Valor Unitário] = 0 ,
                [Total item] =  0  
        from mat.MCT79900 cabecalho
        join mat.MCT80000 item on item.idcotacao = cabecalho.idcotacao
                                        JOIN mat.MXT62300 produto ON
                                            produto.estrut = item.estrut
                                            AND produto.grupo = item.grupo
                                            AND produto.subgrp = item.subgrp
                                            AND produto.itemat = item.itemat
                                            AND produto.digmat = item.digmat
        left join mat.MCT63400 cabreq on cabreq.unges  = cabecalho.unges  and cabreq.anoreq  = cabecalho.anoreq and cabreq.numreq  = cabecalho.numreq                                  
        where year(data_cotacao) >= %d and cabecalho.origem = 'C' and cabecalho.numreq is not null
        union all 
        select 
        cabecalho.idcotacao numero,
        year(cabecalho.data_cotacao) ano,
        data_cotacao dtorc,
        cabecalho.obs descr,
        'NORMAL' prioridade,
        case when cabecalho.codgrupo  is null then
        'Processo de Compra: ' +  cast(cabecalho.numreq as varchar) + '/' + cast(cabecalho.anoreq as varchar)
        else 'Agrupamento: ' +  cast(cabecalho.codgrupo  as varchar) + '/' + cast(cabecalho.anogrupo as varchar) end obs,
        'AP' status,
        'S' liberado,
        coalesce(cabreq.Idnivel5,0) codccusto,
        'L' liberado_tela,
        case cabecalho.origem when 'R' then 'S' else 'N' end registropreco,
                [item] = ROW_NUMBER() over(PARTITION by cabecalho.idcotacao order by cabecalho.idcotacao , itemgr.nuitem),
                [itemorc_ag] = itemgr.nuitem,
                [Requisitante] = coalesce(cabreq.Idnivel5,0) ,
                [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
                [Quantidade] = coalesce(itemgr.quantid, item.qtde)  ,
                [Valor Unitário] = coalesce(itemgr.vlunit,0) ,
                [Total item] = coalesce(itemgr.vlunit  * itemgr.quantid,0) 
        from mat.MCT79900 cabecalho
        join mat.MCT80000 item on item.idcotacao = cabecalho.idcotacao
                                        JOIN mat.MXT62300 produto ON
                                            produto.estrut = item.estrut
                                            AND produto.grupo = item.grupo
                                            AND produto.subgrp = item.subgrp
                                            AND produto.itemat = item.itemat
                                            AND produto.digmat = item.digmat
        left join mat.MCT91300 itemgr on 
        itemgr.codgrupo = cabecalho.codgrupo 
        and itemgr.anogrupo  = cabecalho.anogrupo 
        and itemgr.unges = cabecalho.unges 
        and itemgr.estrut  = item.estrut 
        and itemgr.grupo  = item.grupo
        and itemgr.subgrp  = item.subgrp
        and itemgr.itemat = item.itemat
        and itemgr.digmat = item.digmat
        left join mat.MCT90000 cabreq on cabreq.unges  = itemgr.unges  and cabreq.anoreg  = itemgr.anoreq and cabreq.numreg  = itemgr.numreq                                  
        where year(data_cotacao) >= %d and cabecalho.origem = 'R' and cabecalho.codgrupo is not null
        union ALL 
        select 
        cabecalho.idcotacao numero,
        year(cabecalho.data_cotacao) ano,
        data_cotacao dtorc,
        cabecalho.obs descr,
        'NORMAL' prioridade,
        case when cabecalho.codgrupo  is null then
        'Processo de Compra: ' +  cast(cabecalho.numreq as varchar) + '/' + cast(cabecalho.anoreq as varchar)
        else 'Agrupamento: ' +  cast(cabecalho.codgrupo  as varchar) + '/' + cast(cabecalho.anogrupo as varchar) end obs,
        'AP' status,
        'S' liberado,
        coalesce(cabreq.Idnivel5,0) codccusto,
        'L' liberado_tela,
        case cabecalho.origem when 'R' then 'S' else 'N' end registropreco,
                [item] = ROW_NUMBER() over(PARTITION by cabecalho.idcotacao order by cabecalho.idcotacao , item.nuitem),
                [itemorc_ag] = item.nuitem,
                [Requisitante] = coalesce(cabreq.Idnivel5,0) ,
                [cadpro] = produto.estrut + '.' + produto.grupo + '.' + produto.subgrp + '.' + produto.itemat + '-' + produto.digmat,
                [Quantidade] = item.qtde  ,
                [Valor Unitário] = 0 ,
                [Total item] =  0  
        from mat.MCT79900 cabecalho
        join mat.MCT80000 item on item.idcotacao = cabecalho.idcotacao
                                        JOIN mat.MXT62300 produto ON
                                            produto.estrut = item.estrut
                                            AND produto.grupo = item.grupo
                                            AND produto.subgrp = item.subgrp
                                            AND produto.itemat = item.itemat
                                            AND produto.digmat = item.digmat
        left join mat.MCT90000 cabreq on cabreq.unges  = cabecalho.unges  and cabreq.anoreg  = cabecalho.anoreq and cabreq.numreg  = cabecalho.numreq                                  
        where year(data_cotacao) >= %d and cabecalho.origem = 'R' and cabecalho.numreq is not null
        ORDER by cabecalho.idcotacao , itemgr.nuitem
    """ % (ANO - 5, ANO - 5, ANO - 5, ANO - 5))

    id_cadorc = get_fetchone("SELECT coalesce(max(id_cadorc),0) FROM cadorc")
    ano_atual = 0
    chave_atual = 0
    numero = 0
    numorc = ""

    for row in tqdm(consulta):

        chave_cursor = row['numero']

        if chave_cursor != chave_atual:
            chave_atual = chave_cursor

            if ano_atual != row['ano']:
                ano_atual = row['ano']
                numero = get_fetchone("SELECT COALESCE(CAST(max(num) AS int),0) FROM cadorc WHERE ano = %d" % ano_atual)

            id_cadorc += 1
            numero += 1
            num = f'{numero:05}'
            ano = row['ano']
            numorc = num + '/' + str(ano)[2:4]
            dtorc = row['dtorc']
            descr = row['descr'][:1024] if row['descr'] else row['descr']
            prioridade = row['prioridade']
            obs = row['obs']
            status = row['status']
            liberado = row['liberado']
            codccusto = row['codccusto']
            liberado_tela = row['liberado_tela']
            registropreco = row['registropreco']
            idant = row['numero']

            cur_fdb.execute(insert_cadorc, (
                id_cadorc, num, ano, numorc, dtorc, descr, prioridade, obs, status, liberado, codccusto, liberado_tela,
                EMPRESA, registropreco, idant))

        item = row['item']
        codccusto = row['Requisitante']
        cadpro = PRODUTOS[row['cadpro']]
        qtd = float(row['Quantidade'])
        valor = float(row['Valor Unitário'])
        itemorc = row['item']
        itemorc_ag = row['itemorc_ag']

        cur_fdb.execute(insert_icadorc, (numorc, item, cadpro, qtd, valor, itemorc, codccusto, itemorc_ag, id_cadorc))
    commit()


def fornecedores():
    cur_fdb.execute('delete from fcadorc')
    print("Inserindo Fornecedores das Cotações ...")
    filtro = {}

    registros = cur_sql.execute("""
        select
            c.idcotacao ,
            f.codfor codif ,
            substring(cn.desnom,1,70) nome,
            sum(f.precounit * i.qtde) valor
        from
            mat.MCT79900 c
        join mat.MCT80000 i on
            i.idcotacao = c.idcotacao
        join mat.MCT82100 f on
            f.idcotacaoitem = i.idcotacaoitem
        join mat.MXT60100 cf on
            cf.codfor = f.codfor
        join mat.MXT61400 cn on
            cn.codnom = cf.codnom
        where
            year(data_cotacao) >= %d
        GROUP by
            c.idcotacao ,
            f.codfor ,
            cn.desnom
    """ % (ANO - 5)).fetchall()

    insert = cur_fdb.prep(
        'insert into fcadorc(numorc,codif, nome, valorc, id_cadorc) values (?,?,?,?,?)')

    dados = cur_fdb.execute("SELECT numorc, ID_CADORC , idant FROM CADORC c WHERE idant IS NOT null").fetchallmap()

    for row in dados:
        filtro[row['idant']] = (row['numorc'], row['ID_CADORC'])

    for row in tqdm(registros):
        # filtro = next(x for x in dados if x['idant'] == row.idcotacao)
        numorc = filtro[row.idcotacao][0]  #filtro['numorc']
        codif = row.codif
        nome = row.nome
        valorc = row.valor
        id_cadorc = filtro[row.idcotacao][1]  # filtro['id_cadorc']

        cur_fdb.execute(insert, (numorc, codif, nome, valorc, id_cadorc))

    commit()


def valores():
    print("Inserindo Valores das Cotações ...")


