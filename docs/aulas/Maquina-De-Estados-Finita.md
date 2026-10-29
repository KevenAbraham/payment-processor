# FSM

## Estado 

Só identifica em que ponto do fluxo os dados estão agora. 

## Handler

Função que vai executar o trabalho de um estado e devolve qual é o próximo estado.

## Terminal

Um estado que encerra a execução com sucesso. 

> A máquina interia é só isso. Um ponto de parida, uma coleção de handlers e um jeito de saber quando parar. Nenhuma etapa decide "o que fazer depois" fora do que ela mesma devolve.

```go
Machine[T any] struct {
		initial State 
		handlers map[State]Handler[T]
		terminals map[State]bool 
		fallbacks map[State]State 
		maxHops int 
	}
```

Único tipo genérico declarado. T é o payload que atravessa a máquina de estados, a máquina em si não sabe nada sobre o pagamento ou fraude. 

### initial 
Estado em que toda execução começa. 

### handlers
Têm os handlers com base no estado. State é a chave de acesso para os handlers para saber quais são os handlers de cada estado. Se eu estou começando, quais são os handlers para o estado inicial? Se eu estou no estado terminal, quais são os handlers para o estado terminal?

### terminals
Retorna quais estados encerram a execução. 

### fallbacks
Define para onde ir se o handler falhar. 

### maxhops
Trava de segurança contra loop infinito.

# Funções e Métodos

## `func (m *Machine[T]) Run(ctx context.Context, data *T) ([]Transition, error)`
O motor que anda por baixo de cada estado. Ele verifica a contagem de hops (pulos). Para cada verificação, ele verifica se há algum erro no contexto, pois se eu tenho um erro no contexto, isso significa que alguém deu erro lá na frente de forma concorrente e está me avisando que aconteceu algum problema. 

```go
if ctx.Err() != nil {
	return trace, fmt.Errorf("fsm: context canceled at %q: %w", current, ctx.Err())
}
```

Se não houver problema, eu chamo o handler com o estado atual. Caso dê erro, ele retorna o trace e o erro dizendo que houve um problema de transição. 

```go
handler, ok := m.handlers[current]
if !ok {
	return trace, fmt.Errorf("%w: %q", ErrNoTransition, current)
}
```

Depois, ele verifica qual o próximo handler para quando eu terminar o handler anterior. Caso dê erro, eu verifico qual o fallback para o current (handler atual). Se eu estiver no estado "em andamento" e o fallback for Finalize(), então quando ele der um erro no estado em andamento, ele vai mover esses dados para o estado finalize. Se não houver fallback, ele retorna um erro. 

```go
next, err := handler(ctx, data)
if err != nil {
    fallback, hasFallback := m.fallbacks[current]
    if !hasFallback {
    	return trace, err
    }

    hop.To = fallback
    next = fallback
}
```

E por fim, ele verifica se está no estado terminal atráves do `next`. Caso contrário, ele atribui à variável current o valor de next e ir para a próxima etapa. 

```go
if m.terminals[next] {
	return trace, nil
}

current = next
```

## `func (m *Machine[T]) FallbackTo(states State, target State) *Machine[T]`
Quando um handler falha ou devolve um erro, Run não propaga o erro direto. Primeiro ele consulta se existe um fallback declarado para aquele estado. 

# Como seria o Pipeline de Fraude?
Temos um Machina[scoringData] completo. 

## enrich
Incrementar a contagem de valocity do cartão e resolver o país pelo IP. A busca dde geolocalização é _best effort_, se falhar, segue com o pa~is vazio, nunca aborta a autorização por isso. 

## fast_rules
Aplica regras determinísticas. Se uma regra já decide o veredito com certeza, o handler devolve StateDone direto, pulando modelo e tiebreak inteiros. 

## ml_score
Submete o valor de features e dynamic batcher. Se o batcher não responder a tempo, o handler marca Degraded = true e devolve erro, é esse erro que aciona o FallbackTo (construído aqui no `fsm.go`).

## tiebreak
Só é alcançado quando o score veio ambíguo. Gera um embedding e consulta o cache semântico por uma situação parecida jã decidida antes. 

## finalize
Garante um veredito mesmo se nada mais decidiu (default é revisão manual), grava no cache semântico quando fez sentido e devolve StateDone.


