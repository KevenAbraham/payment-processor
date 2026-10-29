# Como o pipeline de fraude agrupa scorings concorrentes num lote só, sem cada caller saber que isso está acontecendo.

Dynamic Batching é dinamico pois o tamanho do lote não é fixo. 

## Chamada a chamada
Cada requisição concorrente dispara sua propria chamada isolada ao TreeModel.Score, sem nenhuma coordenação com outras chamadas acontecendo ao mesmo tempo.

Sob carga real, com centenas de autorizações concorrentes, isso vira centenas de invocações soltas por segundo, cada uma pagando o proprio overhead de agendamento de goroutine. 

## Agrupadas dinamicamente
Quem chama continua vendo uma chamada sincrona só. Submit, que bloqueia e devolve o proprio resultado.

Por dentro, o Batcher agrupa várias submissões concorrentes num lote só e despacha esse lote de uma vez pro Flusher. 

### Submit Bloqueia, e cada chamador recebe o próprio resultado
Não é fire-and-forget. Quem chama Submit espera o lote inteiro ser processado antes de continuar.

> fire and forget é um termo para disparar e esquecer. Se for fire-and-forget, está tudo bem disparar uma requisição e ela não retornar a tempo. 

