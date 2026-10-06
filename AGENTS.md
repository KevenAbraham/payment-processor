Contexto: payment-processor, sistema de pagamento em Go com 4 microsserviços (gateway, fraud, ledger e investigator), Clean Architecture por servico (domain/usecase/infra/handler) gRPC entre servicos e Kafka para eventos. 

Estilo do código:
- Comentários só quando o porquê não é obvio pelo nome ou pela estrutura. Nunca descreva o que o codigo ja deixa claro. Nunca escreva bloco de doc de multiplos paragrafos, uma ou duas linhas bastam. 
- Sem abstraçoes prematuras: tres linhas parecidas sao melhores que uma abstracao generica cedo demais. So generalize quando o segundo uso real aparecer.
- Erros de negócio são sentinelas (errors.New), comparados com errors.Is, nunca com == depois de qualquer wrapping. Erros de infraestrutura ganham contexto com fmt.Errorf(".." %w, err) ao subir uma camada.
- Generics (type Foo[F any]) só quando o motor é genuinamente independente do dominio (maquina de estados, um cache, um batcher). Nunca como troca de interface{}, tentando evitar escrever o tipo concreto.
- Testes não devem ser table-driven por padrão. Um fake de teste implementa a interface minima da porta, sem framework de mock.~
- Nao devera ter funcoes ou metodos que sao usados somente no codigo.

Arquitetura:
- domain/ não importa nada além de stdlib.
- usecase/ orquestra só contra interface (portas), nunca contra infraestrutura concreta.
- infra/ implementa as portas (Postgres, gRPC, Kafka, HTTP externo)
- adapter/ traduz transporte (proto <=> dominio)
- pkg/ é compartilhado entre servicos e nao conhece nada do dominio de pagamentos

Antes de considerar uma tarefa concluida:
- Rode go build ./... e go vet ./...
- Nao adicione dependencias nova sem justificar por que a stdlib nao resolve.