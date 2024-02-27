from conexao import *
from ..tools import *
from tqdm import tqdm
from re import match

PRODUTOS = produtos()

def cadlic():
    cur_fdb.execute('delete from cadlic')
    cria_campo('ALTER TABLE CADLIC ADD criterio_ant varchar(30)')
    cria_campo('ALTER TABLE CADLIC ADD sigla_ant varchar(2)')
    cria_campo('ALTER TABLE CADLIC ADD status_ant varchar(1)')
    print('Inserindo Cadastro de licitações...')

    i = 0

    insert = cur_fdb.prep("""insert into cadlic (numpro,
                                datae,
                                dtpub,
                                dtenc,
                                horabe,
                                discr,
                                discr7,
                                modlic,
                                dthom,
                                dtadj,
                                comp,
                                numero,
                                ano,
                                registropreco,
                                ctlance,
                                obra,
                                proclic,
                                numlic,
                                liberacompra,
                                microempresa,
                                licnova,
                                tlance,
                                mult_entidade,
                                processo_ano,
                                LEI_INVERTFASESTCE,
                                criterio_ant,
                                sigla_ant,
                                status_ant,
                                codmod,
                                empresa,
                                valor,
                                detalhe)
                            VALUES(?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,
                                ?,?,?,?,?,?,?)""")
    
    consulta = fetchallmap(f"""SELECT RIGHT('000000'+CAST(ROW_NUMBER() OVER (ORDER BY ano) AS VARCHAR), 6)+'/'+SUBSTRING(ano, 3, 2) proclic,
                                    RIGHT('000000'+CAST(ROW_NUMBER() OVER (ORDER BY ano) AS VARCHAR), 6) numero, 
                                    *, 
                                    CASE
                                        when modlic = 'DI01' then 1
                                        when modlic = 'CS01' then 7
                                        WHEN modlic = 'IN01' then 5
                                        when modlic = 'PE01' then 9
                                        when modlic = 'TOM6' then 3
                                        when modlic = 'CON7' THEN 4
                                        WHEN MODLIC = 'CON4' THEN 4
                                        WHEN MODLIC = 'PP01' THEN 8
                                        WHEN MODLIC = 'TOM3' THEN 3
                                END codmod FROM (
                                    SELECT
                                        convit numpro,
                                        dtAtaAberturaProposta datae,
                                        dtpublicresult dtpub,
                                        dtence dtenc,
                                        '00:00' horabe,
                                        substring(ISNULL(ISNULL(notaconv, notaconvtxt), objeto_licitacao),0,1024) discr,
                                        ISNULL(ISNULL(notaconv, notaconvtxt), objeto_licitacao) detalhe,
                                        CASE 
                                            WHEN CriterioJulgamento IN ('Dispensa', 'Inexigibilidade', 'Menor Preço') THEN 'Menor Preço Unitário'
                                            WHEN CriterioJulgamento IN ('Maior Lance ou Oferta', 'Maior Desconto') THEN 'Maior Desconto'
                                            WHEN CriterioJulgamento IN ('Taxa Administrativa') THEN 'Menor Acrescimo'
                                        END discr7,
                                        CASE 
                                            WHEN sigla IN (00) THEN 'DI01' --MODALIDADES DE LICITAÇÃO
                                            WHEN sigla IN (01) THEN 'CS01' --CONCURSO
                                            WHEN sigla IN (02) THEN 'CCO2' --siglae
                                            WHEN sigla IN (03) THEN 'TOM3' --TOMADA 
                                            WHEN sigla IN (04) THEN 'CON4' --CONCORRENCIA
                                            WHEN sigla IN (05) THEN 'DI01' --DISPENSA
                                            WHEN sigla IN (06) THEN 'IN01' --INEXIGIBILIDADE
                                            WHEN sigla IN (07) THEN 'PP01' --Pregão Presencial
                                            WHEN sigla IN (08) THEN 'PE01' --BEC - BOLSA ELETRONICA                            
                                            WHEN sigla IN (09) THEN 'DI01' --OUTROS
                                            WHEN sigla IN (10) THEN 'CCO5' --siglaE (Obras)
                                            WHEN sigla IN (11) THEN 'DI01' --DISPENSA DE LICITAÇÃO - INCISO I                  
                                            WHEN sigla IN (12) THEN 'DI01' --DISPENSA DE LICITAÇÃO - DEMAIS INCISOS            
                                            WHEN sigla IN (13) THEN 'TOM6' --TOMADA DE PREÇOS (OBRAS E SERVIÇOS DE ENGENHARIA) 
                                            WHEN sigla IN (14) THEN 'CON7' --CONCORRÊNCIA (OBRAS E SERVIÇOS DE ENGENHARIA)      
                                            WHEN sigla IN (15) THEN 'DI01' --DISP
                                            WHEN sigla IN (16) THEN 'DI01' --CHAMADA PUB
                                            WHEN sigla IN (17) THEN 'DI01' --DISPENSA DE LICITAÇÃO - LEI Nº 14.133/2021     
                                            WHEN sigla IN (18) THEN 'IN01' --INEXIGIBILIDADE - LEI Nº 14.133/2021    
                                            WHEN sigla IN (19) THEN 'PE01' --Pregão - Eletrônico                                         
                                            WHEN sigla IN (20) THEN 'CON4' --CONCORRÊNCIA ELETRÔNICA
                                            WHEN sigla IN (21) THEN 'LEIL' --LEILÃO
                                            WHEN sigla IN (22) THEN 'DI01' --Pregão Presencial
                                        END modlic,
                                        dtPublicacaoHomologacao dthom,
                                        dataadjudicacao dtadj,
                                        CASE 
                                            --WHEN status IN ('C','R','F','D','H','X','P','O') THEN 3
                                            WHEN status IN ('S') THEN 2
                                            WHEN status IN ('A') THEN 1
                                            ELSE 3
                                        END comp,
                                        anoc ano,
                                        'N' registropreco,
                                        'U' ctlance,
                                        CASE 
                                            WHEN sigla IN (10,13,14) THEN 'S'
                                            ELSE 'N'
                                        END obra,
                                        idagenda numlic,
                                        CASE 
                                            WHEN status = 'H' THEN 'S'
                                            ELSE NULL
                                        END liberacompra,
                                        2 microempresa,
                                        1 licnova,
                                        '$' tlance,
                                        'N' mult_entidade,
                                        anoc processo_ano,
                                        'N' LEI_INVERTFASESTCE,
                                        valorEstimado valor,
                                        CriterioJulgamento criterio_ant,
                                        sigla sigla_ant,
                                        status status_ant
                                    FROM
                                        MAT.MCT67600
                                    WHERE anoc BETWEEN {ANO-5} and {ANO} --anoc >= {ANO}
                                union ALL 
                                SELECT
                                        convit numpro,
                                        dtAtaAberturaProposta datae,
                                        dtpublicresult dtpub,
                                        dtence dtenc,
                                        '00:00' horabe,
                                        substring(ISNULL(ISNULL(objeto_licitacao, notaconv),nota2),0,1024) discr,
                                        ISNULL(ISNULL(objeto_licitacao, notaconv),nota2) detalhe,
                                        'Menor Preço Unitário' discr7,
                                        CASE 
                                            WHEN sigla IN (00) THEN 'DI01' --MODALIDADES DE LICITAÇÃO
                                            WHEN sigla IN (01) THEN 'CS01' --CONCURSO
                                            WHEN sigla IN (02) THEN 'CCO2' --siglae
                                            WHEN sigla IN (03) THEN 'TOM3' --TOMADA 
                                            WHEN sigla IN (04) THEN 'CON4' --CONCORRENCIA
                                            WHEN sigla IN (05) THEN 'DI01' --DISPENSA
                                            WHEN sigla IN (06) THEN 'IN01' --INEXIGIBILIDADE
                                            WHEN sigla IN (07) THEN 'PP01' --Pregão Presencial
                                            WHEN sigla IN (08) THEN 'PE01' --BEC - BOLSA ELETRONICA                            
                                            WHEN sigla IN (09) THEN 'DI01' --OUTROS
                                            WHEN sigla IN (10) THEN 'CCO5' --siglaE (Obras)
                                            WHEN sigla IN (11) THEN 'DI01' --DISPENSA DE LICITAÇÃO - INCISO I                  
                                            WHEN sigla IN (12) THEN 'DI01' --DISPENSA DE LICITAÇÃO - DEMAIS INCISOS            
                                            WHEN sigla IN (13) THEN 'TOM6' --TOMADA DE PREÇOS (OBRAS E SERVIÇOS DE ENGENHARIA) 
                                            WHEN sigla IN (14) THEN 'CON7' --CONCORRÊNCIA (OBRAS E SERVIÇOS DE ENGENHARIA)      
                                            WHEN sigla IN (15) THEN 'DI01' --DISP
                                            WHEN sigla IN (16) THEN 'DI01' --CHAMADA PUB
                                            WHEN sigla IN (17) THEN 'DI01' --DISPENSA DE LICITAÇÃO - LEI Nº 14.133/2021     
                                            WHEN sigla IN (18) THEN 'IN01' --INEXIGIBILIDADE - LEI Nº 14.133/2021    
                                            WHEN sigla IN (19) THEN 'PE01' --Pregão - Eletrônico                                         
                                            WHEN sigla IN (20) THEN 'CON4' --CONCORRÊNCIA ELETRÔNICA
                                            WHEN sigla IN (21) THEN 'LEIL' --LEILÃO
                                            WHEN sigla IN (22) THEN 'DI01' --Pregão Presencial
                                        END modlic,
                                        dtPublicacaoHomologacao dthom,
                                        dataadjudicacao dtadj,
                                        CASE 
                                            --WHEN status IN ('C','R','F','D','H','X','P','O') THEN 3
                                            WHEN status IN ('S') THEN 2
                                            WHEN status IN ('A') THEN 1
                                            ELSE 3
                                        END comp,
                                        anoc ano,
                                        'S' registropreco,
                                        'U' ctlance,
                                        CASE 
                                            WHEN sigla IN (10,13,14) THEN 'S'
                                            ELSE 'N'
                                        END obra,
                                        idagendaRP numlic,
                                        CASE 
                                            WHEN status = 'H' THEN 'S'
                                            ELSE NULL
                                        END liberacompra,
                                        2 microempresa,
                                        1 licnova,
                                        '$' tlance,
                                        'N' mult_entidade,
                                        anoc processo_ano,
                                        'N' LEI_INVERTFASESTCE,
                                        valorEstimado valor,
                                        CriterioAceitabilidade criterio_ant,
                                        sigla sigla_ant,
                                        status status_ant
                                    FROM mat.MCT91400 where anoc between {ANO-5} and {ANO} --anoc >= {ANO}
                                ) AS subconsulta
                                ORDER BY proclic, ANO;""")
    
    for row in tqdm(consulta):
        i += 1
        numpro = int(row['numpro'])
        datae = row['datae']
        dtpub = row['dtpub']
        dtenc = row['dtenc']
        horabe = row['horabe']
        discr = row['discr']
        discr7 = row['discr7']
        modlic = row['modlic']
        dthom = row['dthom']
        dtadj = row['dtadj']
        comp = row['comp']
        numero = row['numero']
        ano = row['ano']
        registropreco = row['registropreco']
        ctlance = row['ctlance']
        obra = row['obra']
        proclic = row['proclic']
        numlic = row['numlic']
        liberacompras = row['liberacompra']
        microempresa = row['microempresa']
        licnova = row['licnova']
        tlance = row['tlance']
        mult_entidade = row['mult_entidade']
        processo_ano = row['processo_ano']
        LEI_INVERTFASESTCE = row['LEI_INVERTFASESTCE']
        criterio_ant = row['criterio_ant']
        sigla_ant = row['sigla_ant']
        status_ant = row['status_ant']
        codmod = row['codmod']
        empresa = EMPRESA
        valor = row['valor']
        detalhe = row['detalhe']

        cur_fdb.execute(insert,(numpro, datae, dtpub, dtenc, horabe, discr, discr7, modlic, dthom, dtadj, comp, numero, ano, registropreco, ctlance, obra, proclic, numlic, liberacompras, microempresa,
                                 licnova, tlance, mult_entidade, processo_ano, LEI_INVERTFASESTCE, criterio_ant, sigla_ant, status_ant, codmod, empresa, valor, detalhe))
        
        if i % 1000 == 0:
            commit()
        
    cur_fdb.execute('''INSERT INTO CADLIC_SESSAO (NUMLIC, SESSAO, DTREAL, HORREAL, COMP, DTENC, HORENC, SESSAOPARA, MOTIVO) 
                  SELECT L.NUMLIC, CAST(1 AS INTEGER), L.DTREAL, L.HORREAL, L.COMP, L.DTENC, L.HORENC, CAST('T' AS VARCHAR(1)), CAST('O' AS VARCHAR(1)) FROM CADLIC L 
                  WHERE numlic not in (SELECT FIRST 1 S.NUMLIC FROM CADLIC_SESSAO S WHERE S.NUMLIC = L.NUMLIC)''')
    commit()

def cadprolic():
    print("Inserindo Itens...")

    