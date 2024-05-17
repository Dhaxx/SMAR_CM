from modulos import *
import conexao

def main():
##### COMPRAS #####
    # compras.base.cadunimedida()
    # compras.base.grupo_e_subgrupo()
    # compras.base.cadest()
    # compras.base.almoxarifado()
    # compras.base.centro_custo()

    # compras.solicitacoes.cadastro()
    # compras.cotacoes.cadastro()
    # compras.cotacoes.fornecedores()
    # compras.cotacoes.valores()

    # compras.licitacao.cadlic()
    # compras.licitacao.cadprolic()
    # tools.fornecedores_gerais()
    # compras.licitacao.prolic_prolics()
    # compras.licitacao.cadpro_proposta()
    # compras.licitacao.cadpro_lance() 
    # compras.licitacao.cadpro_final() 
    # compras.licitacao.cadpro_status() 
    # compras.licitacao.cadpro() 
    # compras.licitacao.regpreco() 
    # compras.licitacao.aditamento()
    # compras.licitacao.fase_v()
    # compras.licitacao.vinculacao_contratos()

    # compras.pedidos.cabecalho()
    # compras.pedidos.itens()
    # compras.estoque.subpedidos()

# ##### ALMOXARIFADO #####   
    # compras.estoque.almoxarif_para_ccusto()
    # compras.estoque.requi_saldo_ant()
    # compras.estoque.requi()

# ##### FROTAS #####
#     conexao.cur_sql.execute('USE SMARfrotas')
#     frotas.motorista.cadastro()
#     frotas.veiculos.modelo()
#     frotas.veiculos.marca()
#     frotas.veiculos.cadastro()   
#     conexao.cur_sql.execute('USE SMARcompras_CM')

##### PATRIMÔNIO #####
    # patrimonio.base.tipos_mov()
#     patrimonio.base.tipos_ajuste()
#     patrimonio.base.tipos_baixa()
#     patrimonio.base.tipos_bens()
#     patrimonio.base.tipos_situacao()
#     patrimonio.base.grupos()
#     patrimonio.base.unidade_subunidade()
#     patrimonio.cadastro.bens()
#     patrimonio.movimentacoes.aquisicao()
#     patrimonio.movimentacoes.ajuste()
#     patrimonio.movimentacoes.baixas()
    # patrimonio.movimentacoes.reavaliacao()
    # patrimonio.movimentacoes.depreciacoes()
    patrimonio.movimentacoes.transferencias()

##### AJUSTES #####
    # tools.aditivos_contratos()
    # tools.insere_cadpro_cadped()
    # compras.licitacao.insere_cadprolic_faltante()
    

if __name__ == '__main__':
    main()