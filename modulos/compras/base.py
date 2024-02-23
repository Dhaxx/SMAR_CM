from conexao import *
from ..tools import * 
from tqdm import tqdm

def cadunimedida():
    cur_fdb.execute("DELETE FROM CADUNIMEDIDA") # Limpa tabela
    cria_campo("ALTER TABLE CADUNIMEDIDA ADD codant_ant INTEGER")

    print("Inserindo Unidades de Medida...")

    cur_sql.execute("SELECT upsigl, rtrim(updesc) as descricao, upcod FROM smar_compras.mat.MCT67900;") # Consulta banco de Origem

    insert = cur_fdb.prep("INSERT INTO CADUNIMEDIDA(sigla, descricao, codant_ant) VALUES(?,?,?)") # Prepara o insert 

    for row in tqdm(cur_sql.fetchall()): # Para cada linha da consulta
        try:
            cur_fdb.execute(insert, (row[0],row[1],row[2])) # Executa o insert
        except Exception as e:
            print(e)
            continue 
    commit() # Salva dados inseridos

def grupo_e_subgrupo():
    cria_campo("ALTER TABLE CADGRUPO ADD estrutura_ant varchar(2)")
    cria_campo("ALTER TABLE CADGRUPO ADD grupo_ant varchar(2)")
    cria_campo("ALTER TABLE CADSUBGR ADD estrutura_ant varchar(2)")
    cria_campo("ALTER TABLE CADSUBGR ADD grupo_ant varchar(2)")
    cria_campo("ALTER TABLE CADSUBGR ADD subgrupo_ant varchar(2)")
    cria_campo("DELETE FROM CADSUBGR")
    cria_campo("DELETE FROM CADGRUPO")
    print("Inserindo Grupos e Subgrupos...")

    consulta = fetchallmap("""select
                            estrut + right('0' + grupo,
                            2) as grupo,
                            right('00' + cast(subgrp as varchar(3)),
                            3) as subgrupo,
                            rtrim(desgrp) as nome,
                            estrut as estrutura_ant,
                            grupo as grupo_ant,
                            subgrp as subgrupo_ant
                        from
                            smar_compras.mat.mxt63300""")

    insert_grupo = cur_fdb.prep("INSERT INTO CADGRUPO(grupo, nome, ocultar, grupo_ant, estrutura_ant) VALUES(?,?,?,?,?)")
    insert_subgrupo = cur_fdb.prep("INSERT INTO CADSUBGR(grupo, subgrupo, nome, ocultar, grupo_ant, subgrupo_ant, estrutura_ant) VALUES(?,?,?,?,?,?,?)")

    for row in tqdm(consulta):
        if row['subgrupo'] == '000':
            cur_fdb.execute(insert_grupo, (row['grupo'],row['nome'],'N',row['grupo_ant'],row['estrutura_ant'])) 
        cur_fdb.execute(insert_subgrupo, (row['grupo'],row['subgrupo'],row['nome'][:45],'N',row['grupo_ant'],row['subgrupo_ant'],row['estrutura_ant']))
    cur_fdb.execute(insert_grupo,('112','Null','N','12','1'))
    commit()

def cadest():
    cria_campo("ALTER TABLE Cadest ADD estrut_ant int")
    cria_campo("ALTER TABLE Cadest ADD grupo_ant varchar(2)")
    cria_campo("ALTER TABLE Cadest ADD subgrupo_ant varchar(2)")
    cria_campo("ALTER TABLE Cadest ADD cod_ant varchar(4)")
    cria_campo("DELETE FROM Cadest") 
    print("Inserindo Cadest...")

    i = 0

    consulta = fetchallmap("""select							
								estrut+grupo as grupo,
								SUBSTRING(itemat,0, 2) + subgrp as subgrp,
								SUBSTRING(itemat, 2, 4) as codigo,
								rtrim(despro) as disc1,
								CASE
									when materialOuServico = 'M' then 'P'
									else 'S'
								END as tipopro,
								rtrim(b.upsigl) as unid1,
								compl_descr as discr1,
								IdMaterial as codreduz,
								CASE
									when desativacompra = 0 then 'S'
									else 'N'
								END as ocultar,
								estrut as estrut_ant,
								grupo as grupo_ant,
								subgrp as subgrupo_ant,
								itemat as cod_ant
							from
								smar_compras.mat.MXT62300 a
							inner join smar_compras.mat.MCT67900 b on
								a.upcod1 = b.upcod;""")
    
    insert = cur_fdb.prep("""INSERT
								INTO
								Cadest(cadpro,
								grupo,
								subgrupo,
								codigo,
								disc1,
								tipopro,
								unid1,
								discr1,
								codreduz,
								ocultar,
								estrut_ant,
								grupo_ant,
								subgrupo_ant,
								cod_ant)
							VALUES(?,?,?,?,?,
								?,?,?,?,?,
								?,?,?,?)""")
    
    for row in tqdm(consulta):
        i += 1
        grupo   = row['grupo']
        subgrp  = row['subgrp']
        codigo  = row['codigo']
        disc1   = row['disc1']
        tipopro = row['tipopro']
        unid1   = row['unid1']
        discr1  = row['discr1']
        codreduz = row['codreduz']
        ocultar = row['ocultar']
        estrut_ant = row['estrut_ant']
        grupo_ant = row['grupo_ant']
        subgrupo_ant = row['subgrupo_ant']
        cod_ant = row['cod_ant']

        if int(cod_ant) >= 1000:
            nome_grupo = extourou_codigo_item(grupo, subgrupo_ant)
            sql = "INSERT INTO cadsubgr (grupo, subgrupo, nome, ocultar, grupo_ant, subgrupo_ant, estrutura_ant) VALUES (?, ?, ?, 'N', ?, ?, ?)"
            try:
                cur_fdb.execute(sql,(grupo, subgrp, nome_grupo, grupo_ant, subgrupo_ant, estrut_ant))
                commit()
            except:
                pass
        
        cadpro = f'{grupo}.{subgrp}.{codigo}'

        cur_fdb.execute(insert, (cadpro, grupo, subgrp, codigo, disc1[:1024], tipopro, unid1, discr1, codreduz, ocultar, estrut_ant, grupo_ant, subgrupo_ant, cod_ant))
        commit() if i % 1000 == 0 else None
    commit()

def almoxarifado():
    cria_campo("ALTER TABLE DESTINO ADD cod_ant varchar(8)")
    cur_fdb.execute("DELETE FROM DESTINO") 
    print("Inserindo Almoxarifado...")

    consulta = fetchallmap(f"select RIGHT('000000000' + replace(almoxarifado, '.', ''),9) as destino, descricao, almoxarifado from mat.AlmoxValid av ")

    insert = cur_fdb.prep("insert into destino (COD, DESTI, EMPRESA, COD_ANT) values (?,?,?,?)")

    for row in tqdm(consulta):
        cur_fdb.execute(insert, (row['destino'], row['descricao'], EMPRESA, row['almoxarifado']))
    commit()

def centro_custo():
    cur_fdb.execute("DELETE FROM CENTROCUSTO")
    cria_campo('ALTER TABLE CENTROCUSTO ADD cod_ant varchar(19)')
    print("Inserindo Centros de Custo...")

    query = fetchallmap(f"""select
                                substring(nivel1,2,4) poder,
                                substring(nivel2,2,4)  orgao,
                                substring(nivel3,2,4) unidade,
                                rtrim(DescrUltimoNivel) descr,
                                rtrim(SiglaUltimoNivel) placa,
                                CASE 
                                    when ativa = 0 then 'S'
                                    else 'N'
                                END ocultar,
                                idNivel5 codccusto,
                                UnidOrc cod_ant
                            from
                                mat.UnidOrcamentariaW
                            where
                                ano = {ANO}""")

    insert = cur_fdb.prep("""insert
                                into
                                centrocusto (poder,
                                orgao,
                                destino,
                                ccusto,
                                descr,
                                obs,
                                placa,
                                codccusto,
                                empresa,
                                unidade,
                                ocultar,
                                cod_ant)
                            values (?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    for row in tqdm(query):
        poder = row['poder']
        orgao = row['orgao']
        destino = '000901001'
        ccusto = '001'
        descr = row['descr'][:60]
        obs = row['descr']
        placa = row['placa'][:7]
        codccusto = row['codccusto']
        empresa = EMPRESA
        unidade = row['unidade']
        ocultar = row['ocultar']
        codant = row['cod_ant']

        cur_fdb.execute(insert, (poder, orgao, destino, ccusto, descr, obs, placa, codccusto, empresa, unidade, ocultar, codant))
    commit()