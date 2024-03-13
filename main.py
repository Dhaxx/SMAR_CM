from modulos.compras import base
from modulos.compras import solicitacoes
from modulos.compras import licitacao
from modulos.compras import cotacoes
from modulos.compras import pedidos
from modulos.compras import estoque
from modulos.frotas import motorista
from modulos.frotas import veiculos
from modulos import tools
import conexao

def main():
    # base.cadunimedida()
    # base.grupo_e_subgrupo()
    # base.cadest()
    # base.almoxarifado()
    # base.centro_custo()
    # solicitacoes.cadastro()
    # cotacoes.cadastro()
    # cotacoes.fornecedores()
    # cotacoes.valores()
    # licitacao.cadlic()
    # licitacao.cadprolic()
    # licitacao.prolic_prolics()
    # licitacao.cadpro_proposta()
    # licitacao.cadpro_lance() 
    # licitacao.cadpro_final() 
    # licitacao.cadpro_status() 
    # licitacao.cadpro() 
    # licitacao.regpreco() 
    licitacao.aditamento()
    licitacao.cadpro_saldo_ant() # Ajustar as demais tabelas / Conferir os processos faltantes
    pedidos.cabecalho()
    pedidos.itens()
    
    estoque.almoxarif_para_ccusto()
    estoque.requi_saldo_ant()
    estoque.requi()

    conexao.cur_sql.execute('USE SMARfrotas')
    motorista.cadastro()
    veiculos.modelo()
    veiculos.marca()
    veiculos.cadastro()   
    conexao.cur_sql.execute('USE smar_compras')

if __name__ == '__main__':
    main()