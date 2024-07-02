from modulos.patrimonio import *

def bens():
    # cur_fdb.execute('delete from pt_cadpat')
    cria_campo('alter table pt_cadpat add cpl_ant varchar(150)')

    consulta = fetchallmap('''
                                SELECT
                                    *
                                from
                                    (
                                    SELECT
                                        codigo_pat = cast(p650.idPatmob as varchar),
                                        codigo_gru_pat = case
                                            when p050.ictipcadastro = 'M' then 1
                                            else 2
                                        end,
                                        chapa_pat = FORMAT(cast(p650.pchapa as integer),'000000'),
                                        codigo_set_pat_pkant = a.UnidOrc,
                                        nota_pat = p650.pdocto,
                                        orig_pat = case
                                            when p650.idformaaquisicao = 22 then 'O'
                                            when p650.idformaaquisicao = 3 then 'C'
                                            when p650.idformaaquisicao = 4 then 'D'
                                        END,
                                        codigo_for_pat = rtrim(c.dcto01),
                                        codigo_tip_pat = p650.idclspatrimonial,
                                        codigo_sit_pat = upper(p810.conceito),
                                        discr_pat = CASE
                                            WHEN EXISTS(
                                            SELECT
                                                1
                                            FROM
                                                mat.MXT70800
                                            WHERE
                                                cgccli = '27142058000126') THEN p650.despro
                                            ELSE RTRIM(LTRIM(ISNULL(p650.despro, '')))+ ' ' + RTRIM(LTRIM(ISNULL(p650.descr, '')))
                                        END,
                                        obs_pat = cast(obsgeral as varchar(max)),
                                        datae_pat = p650.pdtqui,
                                        dt_contabil = p650.pdtlib,
                                        dtpag_pat = p650.datbai,
                                        valaqu_pat = p650.pvalaq,
                                        valatu_pat = p650.pvlmes,
                                        valres_pat = p650.VlrResidual,
                                        tipatr = p050.ictipcadastro,
                                        TpVidaUtil = CASE
                                            p650.tpvidautil WHEN 'D' THEN 'Definido'
                                            ELSE 'Indefinido'
                                        END,
                                        percenqtd_pat = p650.qtmesvidamovel,
                                        case
                                            when p650.tpvidautil = 'D' then 'V'
                                            else NULL
                                        end dae_pat,
                                        Situacao = CASE
                                            p650.icsitmovel         
                                                                                    WHEN 'B' THEN 'Baixado'
                                            WHEN 'I' THEN 'Inservível'
                                            WHEN 'N' THEN 'Normal'
                                            WHEN 'T' THEN 'Transferido'
                                            WHEN 'E' THEN 'Estornado'
                                        END,
                                                                cpl_ant = rtrim(upper(p050.dcclspatrimonial))
                                    FROM
                                        mat.MPT65000 p650
                                        -- Join com a Classe Patrimonial para identificar se é Móvel ou Veículo        
                                    INNER JOIN mat.MPT05000 p050 ON
                                        p050.idclspatrimonial = p650.idclspatrimonial
                                    JOIN mat.MPT81000 p810 ON
                                        p810.idpontfatorinfluencia = p650.idestadocsv
                                    JOIN mat.MXT03700 t037 ON
                                        t037.idformaaquisicao = p650.idformaaquisicao
                                    LEFT join mat.UnidOrcamentariaW a ON
                                        a.idNivel5 = p650.idNivel5
                                    LEFT join mat.MXT60100 b on
                                        b.codfor = p650.codfor
                                    LEFT join mat.mxt61400 c on
                                        c.codnom = b.codnom
                                UNION
                                    SELECT
                                        codigo_pat = cast(p657.idPatImob as varchar),
                                        codigo_gru_pat = 3,
                                        chapa_pat = FORMAT(cast(p657.pinscr as integer),'000000'),
                                        codigo_set_pat_pkant = a.UnidOrc,
                                        nota_pat = rtrim(p657.docto),
                                                                orig_pat = case
                                            when p657.idformaaquisicao = 22 then 'O'
                                            when p657.idformaaquisicao = 3 then 'C'
                                            when p657.idformaaquisicao = 4 then 'D'
                                        end,
                                        codigo_for_pat = rtrim(c.dcto01),
                                        codigo_tip_pat = p657.idclspatrimonial,
                                        codigo_sit_pat = upper(p810.conceito),
                                        discr_pat = RTRIM(LTRIM(p657.pideno)),
                                        obs_pat = observ,
                                        DataAquisicao = p657.pidta1,
                                        DataIncorporacao = p657.pdtlib,
                                        DataBaixa = p657.datbai,
                                        valaqu_pat = p657.pivalt,
                                        valatu_pat = p657.pvlmes,
                                        valres_pat = p657.VlrResidual,
                                        tipatr = 'I',
                                        TpVidaUtil = CASE
                                            p657.tpvidautil WHEN 'D' THEN 'Definido'
                                            ELSE 'Indefinido'
                                        END,
                                        percenqtd_pat = p657.qtmesvidaImovel,
                                        case
                                            when p657.tpvidautil = 'D' then 'V'
                                            else NULL
                                        end dae_pat,
                                        Situacao = CASE
                                            p657.icsitimovel        
                                                                                    WHEN 'B' THEN 'Baixado'
                                            WHEN 'I' THEN 'Inservível'
                                            WHEN 'N' THEN 'Normal'
                                            WHEN 'T' THEN 'Transferido'
                                            WHEN 'E' THEN 'Estornado'
                                        END,
                                        cpl_ant = rtrim(upper(p050.dcclspatrimonial))
                                    FROM
                                        mat.MPT65700 p657
                                    LEFT JOIN mat.MPT81000 p810 ON
                                        p810.idpontfatorinfluencia = p657.idestadocsv
                                    INNER JOIN mat.MPT05000 p050 ON
                                        p050.idclspatrimonial = p657.idclspatrimonial
                                    left JOIN mat.MPT79900 p799  
                                                            ON
                                        p799.dossie = p657.pinscr
                                    JOIN mat.MXT03700 t037 ON
                                        t037.idformaaquisicao = p657.idformaaquisicao
                                    LEFT join mat.UnidOrcamentariaW a ON
                                        a.idNivel5 = p657.idNivel5
                                    LEFT join mat.MXT60100 b on
                                        b.codfor = p657.propriet_ant
                                    LEFT join mat.mxt61400 c on
                                        c.codnom = b.codnom
                                UNION
                                    SELECT
                                        codigo_pat = cast(p801.idAcervo as varchar),
                                        codigo_gru_pat = 4 ,
                                        chapa_pat = format(cast(p801.cod_livro as integer), '000000'),
                                        codigo_set_pat_pkant = a.UnidOrc,
                                        nota_pat = rtrim(p801.iditemnf),
                                                                orig_pat = case
                                            when P801.idformaaquisicao = 22 then 'O'
                                            when P801.idformaaquisicao = 3 then 'C'
                                            when P801.idformaaquisicao = 4 then 'D'
                                        end,
                                        codigo_for_pat = rtrim(c.dcto01),
                                        codigo_tip_pat = p801.idclspatrimonial,
                                        codigo_sit_pat = upper(p810.conceito),
                                        discr_pat = RTRIM(LTRIM(p801.despro)),
                                        obs_pat = observacao,
                                        DataAquisicao = p801.data_inc,
                                        DataIncorporacao = p801.data_incorporacao,
                                        DataBaixa = p801.data_baixa,
                                        valaqu_pat = p801.valor,
                                        valatu_pat = p801.vlbem,
                                        valres_pat = p801.VlrResidual,
                                        tipatr = 'A',
                                        TpVidaUtil = CASE
                                            p801.tpvidautil WHEN 'D' THEN 'Definido'
                                            ELSE 'Indefinido'
                                        END,
                                        percenqtd_pat = p801.qtmesvidaacervo,
                                        dae_pat = case
                                            when p801.tpvidautil = 'D' then 'V'
                                            else NULL
                                        end,
                                        Situacao = CASE
                                            p801.icsitacervo        
                                                                                    WHEN 'B' THEN 'Baixado'
                                            WHEN 'I' THEN 'Inservível'
                                            WHEN 'N' THEN 'Normal'
                                            WHEN 'T' THEN 'Transferido'
                                            WHEN 'E' THEN 'Estornado'
                                        END,
                                        cpl_ant = rtrim(upper(p050.dcclspatrimonial))
                                    FROM
                                        mat.MPT80100 p801
                                    LEFT JOIN mat.MPT81000 p810 ON
                                        p810.idpontfatorinfluencia = p801.idestadocsv
                                    INNER JOIN mat.MPT05000 p050 ON
                                        p050.idclspatrimonial = p801.idclspatrimonial
                                    JOIN mat.MXT03700 t037 ON
                                        t037.idformaaquisicao = p801.idformaaquisicao
                                    LEFT join mat.UnidOrcamentariaW a ON
                                        a.idNivel5 = p801.idNivel5
                                    LEFT join mat.MXT60100 b on
                                        b.codfor = p801.codfor
                                    LEFT join mat.mxt61400 c on
                                        c.codnom = b.codnom
                                UNION
                                    SELECT
                                        codigo_pat = cast(p052.idintangivel as varchar),
                                        codigo_gru_pat = 4 ,
                                        chapa_pat = FORMAT(cast(p052.nrtombo as integer), '000000'),
                                        codigo_set_pat_pkant = a.UnidOrc,
                                        nota_pat = rtrim(p052.iditemnf),
                                                                orig_pat = 'C',
                                        codigo_for_pat = NULL,
                                        codigo_tip_pat = p052.idclspatrimonial,
                                        codigo_sit_pat = 'BOM',
                                        discr_pat = RTRIM(LTRIM(p052.dcintangivel)),
                                        obs_pat = TRIM(dcobsintangivel),
                                        DataAquisicao = p052.dtaqsintangivel,
                                        DataIncorporacao = p052.dtincorpintangivel,
                                        DataBaixa = p052.dtsitintangivel,
                                        valaqu_pat = p052.vraqsintangivel,
                                        valatu_pat = p052.vlbem,
                                        valres_pat = p052.VlrResidual,
                                        tipatr = 'A',
                                        TpVidaUtil = CASE
                                            p052.tpvidautil WHEN 'D' THEN 'Definido'
                                            ELSE 'Indefinido'
                                        END,
                                        percenqtd_pat = p052.qtmesvidaintangivel,
                                        dae_pat = case
                                            when p052.tpvidautil = 'D' then 'V'
                                            else NULL
                                        end,
                                        Situacao = CASE
                                            p052.icsitintangivel        
                                                                                    WHEN 'B' THEN 'Baixado'
                                            WHEN 'I' THEN 'Inservível'
                                            WHEN 'N' THEN 'Normal'
                                            WHEN 'T' THEN 'Transferido'
                                            WHEN 'E' THEN 'Estornado'
                                        END,
                                        cpl_ant = rtrim(upper(p050.dcclspatrimonial))
                                    FROM
                                        mat.MPT05200 p052
                                    INNER JOIN mat.MPT05000 p050 ON
                                        p050.idclspatrimonial = p052.idclspatrimonial
                                    JOIN mat.MXT03700 t037 ON
                                        t037.idformaaquisicao = p052.idformaaquisicao
                                    LEFT join mat.UnidOrcamentariaW a ON
                                        a.idNivel5 = p052.idUnidorc
                                    ) as rn
                                where
                                    [chapa_pat] in (000588, 000589, 000601, 000602, 000603, 000604, 000605, 000606, 000607, 000621, 000622, 000623, 000624, 000625,
                                                     000628, 000659, 000671, 000672, 000703, 000704, 000705, 000706, 000707, 000708, 000709, 000710, 000711, 000712, 
                                                     000713, 000714, 000715, 000716, 000717, 000718, 000743, 000751, 000752, 000755, 000756, 000772, 000773, 000774, 
                                                     000775, 000776, 000777, 000778, 000779, 000780, 000781, 000782, 000783, 000784, 000785, 000786, 000842, 000843, 
                                                     000844, 000845, 000846, 000847, 000848, 000849, 000850, 000851, 000852, 000853, 000859, 000869, 000871, 000872, 
                                                     000873, 000874, 000875, 000893, 000894, 000946, 000947, 000948, 000949, 000950, 000951, 000952, 000953, 000954, 
                                                     000955, 000956, 000957, 000958, 000959, 000960, 000961, 000962, 000963, 000964, 000965, 000966, 000967, 000968, 
                                                     000969, 000970, 000971, 000991, 001028, 001033, 001034, 001035, 001036, 001037, 001040, 001041, 001042, 001043, 
                                                     001044, 001045, 001046, 001047, 001048, 001049, 001050, 001051, 001052, 001053, 001054, 001055, 001056, 001057, 
                                                     001058, 001059, 001060, 001061, 001062, 001063, 001064, 001282, 001283, 001476, 001477, 001531, 001532, 001535, 
                                                     001536, 001758, 001759, 001848, 001849, 001912, 001913, 001916, 001917, 002029, 002030)
                            ''')
    
    insert = cur_fdb.prep("""
                          insert
                                into
                                pt_cadpat (codigo_pat,
                                            empresa_pat,
                                            codigo_gru_pat,
                                            chapa_pat,
                                            codigo_cpl_pat,
                                            codigo_set_pat,
                                            codigo_set_atu_pat,
                                            orig_pat,
                                            codigo_tip_pat,
                                            codigo_sit_pat,
                                            discr_pat,
                                            obs_pat,
                                            datae_pat,
                                            dtlan_pat,
                                            valaqu_pat,
                                            valatu_pat,
                                            codigo_for_pat,
                                            percenqtd_pat,
                                            dae_pat,
                                            valres_pat,
                                            percentemp_pat,
                                            codigo_bai_pat,
                                            dtpag_pat,
                                            nota_pat,
                                            codigo_ant_pat,
                                            cpl_ant)
                            values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                          """)
    
    cadsit = cur_fdb.execute('select descricao_sit, codigo_sit from pt_cadsit').fetchallmap()
    situacoes = {s['descricao_sit']: s['codigo_sit'] for s in cadsit}
    codigo_pat = 2208#cur_fdb.execute('select cast(coalesce(max(codigo_pat), 0) as integer) from pt_cadpat').fetchone()[0]
    _, INSMF = fornecedores()
    
    for row in tqdm(consulta, desc='PATRIMONIO - Cadastro de Bens'):
        codigo_pat += 1
        empresa_pat = EMPRESA
        codigo_gru_pat = row['codigo_gru_pat']
        chapa_pat = row['chapa_pat'].zfill(6)
        codigo_cpl_pat = None  #row['codigo_cpl_pat']
        codigo_set_pat = SUBUNIDADES.get(row['codigo_set_pat_pkant'], 0)
        codigo_set_atu_pat = SUBUNIDADES.get(row['codigo_set_pat_pkant'], 0)
        orig_pat = row['orig_pat']
        codigo_tip_pat = row['codigo_tip_pat']
        codigo_sit_pat = situacoes.get(row['codigo_sit_pat'], 0)
        discr_pat = row['discr_pat'][:255] if row['discr_pat'] else None
        obs_pat = row['obs_pat']
        datae_pat = row['datae_pat'].strftime('%Y-%m-%d') if row['datae_pat'] else None
        dtlan_pat = row['datae_pat'].strftime('%Y-%m-%d') if row['datae_pat'] else None
        valaqu_pat = row['valaqu_pat']
        valatu_pat = row['valatu_pat']
        codigo_for_pat = INSMF.get(row['codigo_for_pat'], 411)
        percenqtd_pat = row['percenqtd_pat']
        dae_pat = row['dae_pat']
        valres_pat = row['valres_pat']
        percentemp_pat = 'M' if dae_pat else None
        dtpag_pat = row['dtpag_pat'].strftime('%Y-%m-%d') if row['dtpag_pat'] else None
        codigo_bai_pat = 2 if dtpag_pat else None
        nota_pat = row['nota_pat']
        codigo_ant_pat = row['codigo_pat']
        cpl_ant = row['cpl_ant'].replace(' DA ', ' PARA ')
        cur_fdb.execute(insert,(codigo_pat, empresa_pat, codigo_gru_pat, chapa_pat, codigo_cpl_pat, codigo_set_pat, codigo_set_atu_pat, 
                                orig_pat, codigo_tip_pat, codigo_sit_pat, discr_pat, obs_pat, datae_pat, dtlan_pat, valaqu_pat, valatu_pat, 
                                codigo_for_pat, percenqtd_pat, dae_pat, valres_pat, percentemp_pat, codigo_bai_pat, dtpag_pat, nota_pat, codigo_ant_pat, cpl_ant))
        commit()
    cur_fdb.execute("""UPDATE pt_cadpat a SET a.CODIGO_CPL_PAT = (SELECT b.balco FROM CONPLA_TCE b WHERE a.cpl_ant = REPLACE(trim(b.titco), '(P)','') AND (b.balco STARTING '123' OR b.balco STARTING '124') AND TIPO <> 'S')""")
    commit()