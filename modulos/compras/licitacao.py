from conexao import *
from ..tools import *
from tqdm import tqdm

PRODUTOS = produtos()

def cadlic():
    print('Inserindo Cadastro de licitações...')

#     select
# 	convit numpro,
# 	dtAtaAberturaProposta datae,
# 	dtpublicresult dtpub,
# 	dtence dtenc,
# 	isnull(notaconv,  notaconvtxt) discr,
# 	CriterioJulgamento discr7,
# 	CASE 
# 		when convit in (14, 20) then 'CONCE' 
# 		when convit in (05, 11, 12) then 'DISP' --DISPENSA
# 		when convit in (01) then 'CS' --CONCURSO
# 		when convit in (03,13) then 'TP' --TOMADA 
# 		when convit in (04) then 'CONC' --CONCORRENCIA
# 		when convit in (06) then 'INEX' --INEXIGIBILIDADE
# 		when convit in (07) then 'PP' --Pregão Presencial
# 		when convit in 
# 	END
# from
# 	MAT.MCT67600
# where
# 	anoc = 2023