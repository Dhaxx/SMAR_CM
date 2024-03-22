from modulos.patrimonio import *

def bens():
    cur_fdb.execute('delete from pt_cadpat')

    consulta = fetchallmap('''
                            select
                                idPatMob as codigo_pat,
                                right(c.pchapa,
                                6) as chapa_pat,
                                substring(rtrim(replace(c.despro , char(9), '')), 1, 255) as discr_pat,
                                obsgeral,
                                1 as codigo_gru_pat,
                                case
                                    when idformaaquisicao = 3 then 'C'
                                    when idformaaquisicao = 4 then 'D'
                                    else 'O'
                                end orig_pat,
                                c.idclspatrimonial as codigo_tip_pat,
                                c.idestadocsv,
                                c.idclspatrimonial as codigo_cpl_pat,
                                coalesce (c.codfor,
                                0) as codigo_for_pat,
                                nivel1 + '.' + nivel2 + '.' + nivel3 + + '.' + nivel4 + '.' + nivel5 as codigo_set_pat_pkant,
                                cast(c.pvalaq as float) as valaqu_pat,
                                cast(c.pvlmes as float) as valatu_pat,
                                c.qtmesvidamovel as percenqtd_pat,
                                case
                                    when c.qtmesvidamovel <> 0 then 'V'
                                    else Null
                                end dae_pat,
                                (cast(c.vlrresidual as float)) as valres_pat,
                                case
                                    when c.qtmesvidamovel <> 0 then 'M'
                                    else null
                                end percentemp_pat,
                                c.pdtqui as datae_pat,
                                c.pdtlib as dtlan_pat,
                                case
                                    when c.codbaixa = 0 then null
                                    else c.codbaixa
                                end as codigo_bai_pat,
                                cast(c.datbai as date) as dtpag_pat,
                                right('0000000000000' + pdocto,
                                20) nota_pat
                            from
                                mat.mpt65000 c
                            left join mat.mpt68600 d on
                                c.tcodig = d.tcodig
                            left join mat.mxt71100 e on
                                e.idnivel5 = c.idnivel5
                            order by
                                c.pchapa
                            ''')
    
    insert = cur_fdb.prep('''
                          insert
                                into
                                pt_cadpat (codigo_pat,
                                empresa_pat,
                                chapa_pat,
                                discr_pat,
                                obs_pat,
                                codigo_gru_pat,
                                orig_pat,
                                codigo_tip_pat,
                                codigo_sit_pat,
                                codigo_cpl_pat,
                                codigo_for_pat,
                                codigo_set_pat,
                                codigo_set_atu_pat,
                                valaqu_pat,
                                valatu_pat,
                                percenqtd_pat,
                                dae_pat,
                                valres_pat,
                                percentemp_pat,
                                datae_pat,
                                dtlan_pat,
                                codigo_bai_pat,
                                dtpag_pat,
                                nota_pat)
                            values(?,?,?,?,?,?,
                                   ?,?,?,?,?,?,
                                   ?,?,?,?,?,?,
                                   ?,?,?,?,?,?);
                          ''')
    
    for row in tqdm(consulta, desc='PATRIMONIO - Cadastro de Bens'):
        codigo_pat = row['codigo_pat']
        empresa_pat = EMPRESA
        codigo_gru_pat = row['codigo_gru_pat']
        chapa_pat = row['chapa_pat']
        codigo_cpl_pat = row['codigo_cpl_pat']
        codigo_set_pat = SUBUNIDADES.get(row['codigo_set_pat_pkant'], 0)
        codigo_set_atu_pat = SUBUNIDADES.get(row['codigo_set_pat_pkant'], 0)
        orig_pat = row['orig_pat']
        codigo_tip_pat = row['codigo_tip_pat']
        codigo_sit_pat = row['codigo_sit_pat']
        discr_pat = row['discr_pat']
        obs_pat = row['obsgeral']
        datae_pat = row['datae_pat']
        dtlan_pat = row['dtlan_pat']
        valaqu_pat = row['valaqu_pat']
        valatu_pat = row['valatu_pat']
        codigo_for_pat = row['codigo_for_pat']
        percenqtd_pat = row['percenqtd_pat']
        dae_pat = row['dae_pat']
        valres_pat = row['valres_pat']
        percentemp_pat = row['percentemp_pat']
        codigo_bai_pat = row['codigo_bai_pat']
        dtpag_pat = row['dtpag_pat']
        nota_pat = row['nota_pat']
        cur_fdb.execute(insert,(codigo_pat, empresa_pat, chapa_pat, discr_pat, obs_pat, codigo_gru_pat, orig_pat, 
                                codigo_tip_pat, codigo_sit_pat, codigo_cpl_pat, codigo_for_pat, codigo_set_pat, 
                                codigo_set_atu_pat, valaqu_pat, valatu_pat, percenqtd_pat, dae_pat, valres_pat, 
                                percentemp_pat, datae_pat, dtlan_pat, codigo_bai_pat, dtpag_pat, nota_pat))
    cur_fdb.execute('UPDATE pt_cadpat a SET a.CODIGO_CPL_PAT = (SELECT b.codigo_tce_tip FROM pt_cadtip b WHERE b.codigo_tip = a.CODIGO_TIP_PAT AND b.codigo_tce_tip IS NOT null)')
    commit()