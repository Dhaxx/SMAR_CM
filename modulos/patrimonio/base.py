from patrimonio import *

def tipos_mov():
    cur_fdb.execute('delete pt_tipomov')
    
    valores = [
        ("A", "AQUISIÇÃO"),
        ("B", "BAIXA"),
        ("T", "TRANSFERÊNCIA"),
        ("R", "PR. CONTÁBIL"),
        ("P", "TRANS. PLANO")]
    
    cur_fdb.executemany('insert into pt_tipomov (codigo_tmv, descricao_tmv) values (?, ?)', valores)
    commit()

def tipos_ajuste():
    print ("Inserindo Tipos de Ajuste")
    cur_fdb.execute("""delete from pt_cadajuste""")
    insert = cur_fdb.prep("INSERT INTO PT_CADAJUSTE (CODIGO_AJU, EMPRESA_AJU, DESCRICAO_AJU) VALUES (?, ?, ?)")
    
    valores = [(1, EMPRESA, "REAVALIAÇÃO(ANTES DO CORTE)")]

    cur_fdb.executemany(insert, valores)
    commit()

def tipos_baixa():
    cur_fdb.execute('delete pt_cadbai')
    cria_campo('alter table pt_cadbai add codsis_ant varchar(20)')
    cria_campo('alter table pt_cadbai add codtab_ant varchar(20)')
    cria_campo('alter table pt_cadbai add codite_ant varchar(20)')
    
    consulta = fetchallmap('''
        SELECT
            a.codsis,
            a.codtab,
            a.codite,
            a.descrc
        FROM
            mat.MXT60500 A
        WHERE
            a.codsis = '003'
            AND a.codtab = '014'
            AND A.CODITE <> '00'
        ORDER BY
            1,
            2
    ''')

    insert = cur_fdb.prep("INSERT INTO PT_CADBAI (CODIGO_BAI, EMPRESA_BAI, DESCRICAO_BAI, CODSIS_ANT, CODTAB_ANT, CODITE_ANT) VALUES (?, ?, ?, ?, ?, ?)")

    i = 0

    for row in tqdm(consulta, desc="PATRIMÔNIO - Tipos de Baixa"):
        i += 1
        codigo_bai = i
        empresa_bai = EMPRESA
        descricao_bai = row['DESCRC']
        codsis_ant = row['CODSIS']
        codtab_ant = row['CODTAB']
        codite_ant = row['CODITE']
        valores = (codigo_bai, empresa_bai, descricao_bai, codsis_ant, codtab_ant, codite_ant)
        cur_fdb.execute(insert, valores)
    commit()

def tipos_bens():
    cur_fdb.execute('delete pt_cadtip')

    cria_campo('alter table pt_cadtip add ID_IDCLSPATRIMONIAL varchar(20)')
    cria_campo('alter table pt_cadtip add ID_GRPBENS varchar(20)')
    cria_campo('alter table pt_cadtip add ID_CDCLASSE varchar(20)')
    cria_campo('alter table pt_cadtip add ID_ICTIPCADASTRO varchar(20)')

    consulta = fetchallmap('''
        SELECT a.idclspatrimonial, a.grpbens, a.cdclasse, a.dcclspatrimonial, A.ictipcadastro
        FROM MAT.MPT05000 A
        ORDER BY 1, 2, 3
    ''')

    insert = cur_fdb.prep('insert into pt_cadtip (codigo_tip, empresa_tip, descricao_tip, ID_IDCLSPATRIMONIAL, id_grpbens, id_cdclasse, id_ictipcadastro) values (?, ?, ?, ?, ?, ?, ?)')
    codigo_tip = 0

    for row in tqdm(consulta, desc="PATRIMÔNIO - Tipos de Bens"):
        codigo_tip += 1
        empresa_tip = EMPRESA
        descricao_tip = row['DCCLSPATRIMONIAL']
        id_idclspatrimonial = row['IDCLSPATRIMONIAL']
        id_grpbens = row['GRPBENS']
        id_cdclasse = row['CDCLASSE']
        id_ictipcadastro = row['ICTIPCADASTRO']
        valores = (codigo_tip, empresa_tip, descricao_tip, id_idclspatrimonial, id_grpbens, id_cdclasse, id_ictipcadastro)
        cur_fdb.execute(insert, valores)
    commit()

def tipos_situacao():
    cur_fdb.execute('delete pt_cadsit')   

    cria_campo('alter table pt_cadsit add id_codsis varchar(20)')
    cria_campo('alter table pt_cadsit add id_codtab varchar(20)')
    cria_campo('alter table pt_cadsit add id_codite varchar(20)')   

    consulta = fetchallmap('''
        SELECT
            a.codsis,
            a.codtab,
            a.codite,
            a.descrc
        FROM
            mat.MXT60500 A
        WHERE
            a.codsis = '003'
            AND a.codtab = '001'
            AND A.CODITE <> '00'
        ORDER BY
            1,
            2
    ''')

    insert = cur_fdb.prep("INSERT INTO PT_CADSIT (CODIGO_SIT, EMPRESA_SIT, DESCRICAO_SIT, ID_CODSIS, ID_CODTAB, ID_CODITE) VALUES (?, ?, ?, ?, ?, ?)")
    i = 0

    for row in tqdm(consulta, desc="PATRIMÔNIO - Tipos de Situação"):
        i += 1
        codigo_sit = i
        empresa_sit = EMPRESA
        descricao_sit = row['DESCRC']
        id_codsis = row['CODSIS']
        id_codtab = row['CODTAB']
        id_codite = row['CODITE']
        valores = (codigo_sit, empresa_sit, descricao_sit, id_codsis, id_codtab, id_codite)
        cur_fdb.execute(insert, valores)
    commit()

def grupos():
    cur_fdb.execute('delete pt_cadpatg')        

    insert = cur_fdb.prep("INSERT INTO PT_CADPATG (CODIGO_GRU, EMPRESA_GRU, NOGRU_GRU) VALUES (?, ?, ?)")
    valores = [(1, EMPRESA, "Geral")]     

    cur_fdb.execute(insert, valores)
    commit()
        
def unidade():
    cur_fdb.execute('delete pt_cadpatd')
    cria_campo('alter table pt_cadpatd add pkant varchar(20)')

    insert = cur_fdb.prep(""" INSERT INTO PT_CADPATD ( codigo_des, empresa_des, nauni_des, ocultar_des, pkant ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """)

    consulta = fetchallmap(f'''
                           SELECT DISTINCT A.NIVEL1 AS PKANT, A.descr3 AS NAUNI_DES, 'N' AS OCULTAR_DES
                            FROM MAT.MXT70700 A
                            WHERE ANO = {ANO}
                            ORDER BY 1, 2''')
    
    codigo_des = 0

    for row in tqdm(consulta, desc="PATRIMÔNIO - Unidades"):
        codigo_des += 1
        empresa_des = EMPRESA
        nauni_des = row['NAUNI_DES']
        ocultar_des = row['OCULTAR_DES']
        pkant = row['PKANT']
        valores = (codigo_des, empresa_des, nauni_des, ocultar_des, pkant)
        cur_fdb.execute(insert, valores)
    commit()

def subunidade():
    cur_fdb.execute('delete pt_cadpatd')
    cria_campo('alter table pt_cadpatd add pkant varchar(20)')
    cria_campo('alter table pt_cadpatd add pkant_cod varchar(20)')

    insert = cur_fdb.prep(""" INSERT INTO PT_CADPATD (codigo_set, codigo_des_set, noset_set, ocultar_set, pkant, pkant_cod) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """)
    
    consulta = fetchallmap(f'''
        SELECT DISTINCT A.NIVEL1 AS CODIGO_DES_SET_PKANT, CONCAT(A.NIVEL1,'|',A.NIVEL2,'|',A.NIVEL3) AS PKANT,
        A.IDNivel3 AS PKANT_COD, A.descr3 AS NOSET_SET, 'N' AS OCULTAR_SET
        FROM MAT.MXT70700 A
        WHERE ANO = {ANO}
        ORDER BY 1, 2
    ''')

    codigo_set += 1

    for row in tqdm(consulta, desc='PATRIMÔNIO - Subunidades'):
        codigo_set += 1
        codigo_des_set = UNIDADES[row['CODIGO_DES_SET_PKANT']]
        noset_set = row['NOSET_SET']
        ocultar_set = row['OCULTAR_SET']
        pkant = row['PKANT']
        pkant_cod = row['PKANT_COD']
        valores = (codigo_set, codigo_des_set, noset_set, ocultar_set, pkant, pkant_cod)
        cur_fdb.execute(insert, valores)
    commit()