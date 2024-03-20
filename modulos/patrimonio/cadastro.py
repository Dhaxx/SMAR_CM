from modulos.patrimonio import *

def bens():
    cur_fdb.execute('delete from pt_cadpat')

    consulta = fetchallmap('''
                            select
                                c.pchapa as codigo_pat,
                                right(c.pchapa,
                                6) as chapa_pat,
                                replace(c.despro , char(9), '') as discr_pat,
                                1 as codigo_gru_pat,
                                'i' as orig_pat,
                                c.idclspatrimonial as codigo_tip_pat,
                                c.icsitmovel as codigo_sit_pat,
                                c.idclspatrimonial as codigo_cpl_pat,
                                coalesce (c.codfor,
                                0) as codigo_for_pat,
                                nivel1 + '.' + nivel2 + '.' + nivel3 + + '.' + nivel4 + '.' + nivel5 as codigo_set_pat_pkant,
                                cast(c.pvalaq as float) as valaqu_pat,
                                cast(c.pvlmes as float) as valatu_pat,
                                c.qtmesvidamovel as percenqtd_pat,
                                (cast(c.vlrresidual as float)) as valres_pat,
                                c.pdtqui as datae_pat,
                                c.pdtlib as dtlan_pat,
                                c.codbaixa as codigo_bai_pat,
                                cast(c.datbai as date) as dtpag_pat
                            from
                                mat.mpt65000 c
                            left join mat.mpt68600 d on
                                c.tcodig = d.tcodig
                            left join mat.mxt71100 e on
                                e.idnivel5 = c.idnivel5
                            order by
                                c.pchapa
                            ''')
    
    insert = cur_fdb.prep('''insert
                                into
                                pt_cadpat (codigo_pat,
                                empresa_pat,
                                chapa_pat,
                                discr_pat,
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
                                valres_pat,
                                datae_pat,
                                dtlan_pat,
                                codigo_bai_pat,
                                dtpag_pat)
                            values(?,?,?,?,?,?,
                                   ?,?,?,?,?,?,
                                   ?,?,?,?,?,?,
                                   ?,?);''')
    
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
        datae_pat = row['datae_pat']
        dtlan_pat = row['dtlan_pat']
        valaqu_pat = row['valaqu_pat']
        valatu_pat = row['valatu_pat']
        codigo_for_pat = row['codigo_for_pat']
        percenqtd_pat = row['percenqtd_pat']
        valres_pat = row['valres_pat']
        codigo_bai_pat = row['codigo_bai_pat']
        dtpag_pat = row['dtpag_pat']
        cur_fdb.execute(insert,(codigo_pat, empresa_pat, chapa_pat, discr_pat, codigo_gru_pat, orig_pat, codigo_tip_pat, codigo_sit_pat, codigo_cpl_pat, codigo_for_pat, codigo_set_pat, codigo_set_atu_pat, valaqu_pat, valatu_pat, percenqtd_pat, valres_pat, datae_pat, dtlan_pat, codigo_bai_pat, dtpag_pat))
    commit()