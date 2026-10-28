# Por que não Redis?

## Com redis
- Cada velocity check e cada consulta ao cache semantico vira uma ida de rede, tipicamente ~0.5ms memso em rede local, contra ~50ns de acesso a memoria do proprio processo.
- Mais um componente para subir, monitorar e ter disponibilidade propria, dentro do caminho mais sensivle a latencia do sistema inteiro. 
- Uma falha do redis vira uma falha do caminho de autorizacao, nao so uma degradacao. 

## Em processo 
- Velocity e cache semantico vivem na memoria do proprio servico de fraude, acesso local, sem round trip nenhum. 
- Nada novo para operar, nenhuma dependencia externa nova no caminho critico
- O preço: estado nao é compartilhado entre replicas do servico cada uma tem sua propria visao. 

