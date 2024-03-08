from conexao import *
from ..tools import *
from tqdm import tqdm

def almoxarif_para_ccusto():
    consulta = cur_fdb.execute("""select cod_ant, cod destino, cast(cod as integer) as codccusto, empresa, desti descr from destino""").fetchallmap()
    insert = cur_fdb.prep("""insert into centrocusto (poder, orgao, unidade, destino, ccusto, descr, empresa, codccusto, ocultar) values (?,?,?,?,?,?,?,?,?)""")

    for row in tqdm(consulta, desc='Definindo cada almoxarifado como pseudo-centro de custo'):
        poder_orgao_unidade = row['cod_ant'].split('.')
        
        poder = poder_orgao_unidade[0].zfill(2)
        orgao = poder_orgao_unidade[1].zfill(2)
        unidade = poder_orgao_unidade[2][1:]
        destino = row['destino']
        ccusto = '001'
        descr = f"ALMOXARIFADO - {row['descr']}"
        empresa = row['empresa']
        codccusto = row['codccusto']
        cur_fdb.execute(insert,(poder, orgao, unidade, destino, ccusto, descr, empresa, codccusto, 'N'))
    commit()

def requi_saldo_ant():
    cur_fdb.execute("delete from requi")
    cur_fdb.execute("delete from icadreq")

    consulta = fetchallmap("""""")

    insert_requi = cur_fdb.prep("""INSERT INTO requi (EMPRESA, ID_REQUI, requi, num, ano, destino, CODCCUSTO, DTLAN,
                            DATAE, ENTR, said, COMP, TIPOSAIDA, TPREQUI, obs) 
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    insert_icadreq = cur_fdb.prep("""insert into icadreq (id_requi, requi, codccusto, empresa, item, quan1, vaun1, vato1, quan2, vaun2, vato2, cadpro, destino) 
                                values (?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    cur_fdb.execute(insert_requi,(EMPRESA, 1, f'000000/{ANO/2000}','000000',ANO, '000901001', '901001', '2024-01-01', '2024-01-01', 'S', 'S', 'P', 'P', 'OUTRA', 'SALDO ANTERIOR'))

    for row in tqdm(consulta):