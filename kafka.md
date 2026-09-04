# Kafka

## O problema que o Kafka resolve

Dado só tem valor de verdade dependendo do contexto: **quando** ele chega (detectar uma fraude 1 minuto depois é bem diferente de detectar 1 semana depois) e **com o que ele se relaciona** (venda do dia, da semana, do ano). Não existe uma ferramenta única que resolva isso para qualquer tipo de dado.

Imagine um sistema simples: um site manda logs direto pra um banco que já faz a análise. Funciona até que:

- o tráfego aumenta muito (uma promoção, por exemplo) e o banco não aguenta;
- o banco cai e ninguém mais consegue mandar nada;
- um erro faz parte dos logs se perderem.

Esse é o padrão de problema que aparece toda vez que sistemas trocam dados diretamente: informação duplicada sendo mandada pra todo lugar, sistemas trabalhando em ritmos diferentes, indisponibilidade que trava tudo, e risco de perder dado ou ter que reprocessar.

## O que é Apache Kafka

Kafka é uma plataforma de streaming de eventos, distribuída e open source, usada para mover grandes volumes de dados entre sistemas com alta performance.

Pensa em um serviço de streaming de música: os artistas (produtores) publicam suas músicas uma única vez num lugar central; quem gosta daquele estilo (consumidores) assina e escuta quando quiser. Ninguém manda música direto pra ninguém.

O Kafka funciona assim: **producers** publicam dados uma única vez, e **consumers** assinam só o que interessa a eles. Isso traz algumas vantagens importantes:

- Produtor e consumidor ficam desacoplados — um não precisa saber do outro;
- Cada lado trabalha no seu próprio ritmo;
- Um consumidor pode reler os mesmos dados quantas vezes quiser;
- Se o produtor cair, isso não trava quem está consumindo (os dados publicados continuam disponíveis);
- Como o Kafka roda em cluster e divide os dados em partições, ele aguenta bastante volume e continua disponível mesmo se uma parte falhar.

## Sistemas distribuídos: a base de tudo isso

Quando um único servidor não dá mais conta de processar os dados, existem dois caminhos:

**Escalar verticalmente** (colocar um servidor maior): tem um teto baixo, exige parar o sistema pra trocar de máquina, configuração fica complexa e podem surgir problemas de compatibilidade.

**Escalar horizontalmente** (somar mais servidores): capacidade praticamente ilimitada, não precisa parar nada pra crescer, manutenção mais simples, funciona com hardware comum, e ainda ganha tolerância a falhas — se uma máquina cai, as outras seguram a onda.

Um **cluster** nada mais é do que várias máquinas conectadas trabalhando pelo mesmo objetivo — diferente de uma rede comum, onde cada máquina faz uma coisa diferente (e-mail, storage, ERP...).

Duas técnicas sustentam um cluster:

- **Particionamento**: em vez de guardar todos os dados num lugar só, divide-se fisicamente os dados (por exemplo, vendas por região do país). Assim, uma consulta só precisa varrer a fatia relevante, não a base inteira. Só que isso sozinho cria um problema: se a máquina daquela partição cai, você perde o acesso àquele pedaço dos dados.
- **Replicação**: pra resolver a fragilidade do particionamento, cada partição ganha cópias em outras máquinas. O modelo mais comum é master/slave: a cópia master recebe as escritas e replica para as slaves, que atendem as leituras. Se uma cópia cai, as outras continuam respondendo — essa é a base da alta disponibilidade.

## Os componentes do Kafka

- **Kafka Broker** — o servidor que efetivamente guarda e serve os dados;
- **Kafka Client API** — biblioteca para produzir/consumir mensagens via código;
- **Kafka Connect** — conectores prontos para integrar com outros sistemas sem escrever código;
- **Kafka Streams** — processamento de dados em tempo real dentro do próprio Kafka;
- **Kafka KSQL** — consultas em estilo SQL sobre os streams.

## Cluster, Brokers e Partições

Um cluster Kafka é formado por vários **brokers**, e cada nó do cluster roda uma ou mais instâncias de broker (um mesmo servidor pode até hospedar mais de um serviço).

O que um broker faz:

- Dá um número sequencial (**offset**) para cada mensagem que chega;
- Grava tudo em disco — a mensagem não precisa ser consumida na hora, ela fica lá esperando, o que também permite recuperação em caso de falha do consumidor.

**Bootstrap**: o cliente não precisa saber como o cluster inteiro está montado. Basta se conectar a **um** broker qualquer; esse broker já informa a estrutura completa, e a partir daí o cliente pode falar diretamente com o broker certo. Ou seja, conectar em um broker é, na prática, conectar no cluster todo.

Um broker pode guardar vários tópicos, e cada tópico pode ter várias partições — mas nem todo broker precisa ter partições de todo tópico.

**Controller**: é simplesmente o primeiro broker que entrou no cluster, e ele acumula tarefas administrativas extras. Se ele cai, os outros brokers disputam para assumir esse papel, mas só um vence a eleição. Curiosidade: se o broker antigo voltar, ele não recupera o posto de controller automaticamente.

### Leader e Follower

Cada partição tem uma cópia principal, chamada **Leader**, e cópias reservas chamadas **Followers** (a quantidade de followers depende do fator de replicação configurado). Só o Leader atende pedidos de leitura e escrita — o producer manda a mensagem pra ele, ele grava e confirma o recebimento (acknowledgment), e o consumer também lê dali. Os Followers ficam quietos, só copiando o que o Leader recebe, prontos para assumir se ele cair.

Na hora de distribuir as réplicas pelas máquinas, o Kafka tenta balancear a carga e sempre coloca cópias de uma mesma partição em máquinas (e até racks) diferentes — assim, se um rack inteiro cair, ainda sobra cópia em outro lugar.

### ISR (In-Sync Replicas)

O Leader mantém uma lista chamada **ISR**, com as réplicas que estão realmente atualizadas. Uma réplica cai dessa lista quando fica muito tempo (por padrão, 10 segundos) sem buscar as mensagens mais recentes — e só quem está na lista pode ser eleito o próximo Leader se o atual falhar. Isso evita que uma réplica desatualizada vire líder e "perca" mensagens recentes.

É possível configurar o Leader para só considerar uma mensagem confirmada (*committed*) depois que ela for copiada para todas as réplicas da lista ISR. Assim, se o Leader cair, só se perdem as mensagens ainda não confirmadas — e o producer, que fica esperando a confirmação, reenvia elas.

Dá também para definir um número mínimo de réplicas sincronizadas (**minimum ISR**). Se esse mínimo não for atingido, o Leader passa a recusar novas escritas e vira "somente leitura" — prioriza não perder dado a continuar aceitando gravações.

**Replication factor** é simplesmente quantas cópias cada partição vai ter. Se um tópico tem 4 partições com fator de replicação 3, na prática existem 12 cópias de partição espalhadas pelos brokers disponíveis.

### Como os dados ficam armazenados fisicamente

Cada partição vira um diretório em disco chamado "log", e esse diretório é dividido em arquivos menores chamados **segments**. As mensagens vão entrando no segmento atual até ele bater um limite (por padrão, 1 GB ou 7 dias) — aí um novo segmento é aberto. O nome do arquivo do segmento corresponde ao primeiro offset que ele contém.

O **offset** é o número sequencial que identifica uma mensagem — mas atenção: ele não reinicia a cada novo segmento, e não é único no tópico como um todo, e sim dentro de cada partição. Ele serve pra manter o estado de leitura, permitir recomeçar do ponto certo e identificar uma mensagem de forma única (junto com tópico e número da partição).

Existe ainda um arquivo auxiliar chamado **timeindex**, que permite buscar mensagens por intervalo de tempo em vez de por offset.

## Zookeeper

Em sistemas distribuídos, geralmente é preciso um "gestor" — um nó eleito que cuida de recursos, controla quem entra e sai do cluster e lida com falhas. O próprio Kafka já tem o **controller** para parte disso, mas historicamente ele dependia de uma ferramenta externa para gerenciamento: o **Zookeeper**.

O Zookeeper é um projeto open source da Apache, usado em vários sistemas distribuídos, não só no Kafka. Pode-se rodar um cluster de Zookeeper, onde cada instância cuida de um ou mais brokers, com no máximo um líder e zero ou mais seguidores. Ele era obrigatório nas versões mais antigas do Kafka, mas a tendência é ele deixar de ser necessário em versões futuras.

## Topics e Mensagens

Um **Topic** é basicamente um "assunto" — algo parecido com uma tabela de banco de dados, só que **imutável**: uma vez publicada, a mensagem não muda. Dá pra ter vários tópicos diferentes (por exemplo, log de acesso, log de erro, log de agente, log de referência), cada um recebendo mensagens de vários produtores e sendo lido por vários consumidores interessados.

Uma **mensagem** (também chamada de evento ou record) carrega o conteúdo em si e pode ter uma **chave** (key). A retenção das mensagens é configurável — por tempo, por tamanho do tópico, ou até guardando só a versão mais recente de cada chave (chamado *log compacted*).

### Partições de um tópico

Uma **partição** é um pedaço menor do tópico, armazenada em um único nó do cluster. As mensagens vão sendo distribuídas entre as partições (elas não precisam ter a mesma quantidade de mensagens). O número de partições é definido pelo administrador na criação e **não pode ser dividido depois**. Regra importante: só pode existir um consumidor lendo cada partição por vez, então nunca adianta ter mais consumidores do que partições.

Na prática, uma partição não passa de um diretório usado para organizar os dados fisicamente — pode estar no mesmo servidor de outra partição do mesmo tópico, ou em servidores diferentes.

As mensagens são produzidas e lidas em ordem **dentro de uma mesma partição** — não existe garantia de ordem entre partições diferentes, mesmo sendo do mesmo tópico.

O **offset** identifica cada mensagem dentro da sua partição: é gerado automaticamente, é imutável e segue a ordem de chegada. Os offsets confirmados (commitados) por um consumidor ficam registrados num tópico interno especial chamado `__consumer_offsets` — é isso que permite o consumidor parar e recomeçar exatamente de onde tinha ficado. Juntando tópico + número da partição + offset, dá pra identificar qualquer mensagem de forma única.

### Montando uma mensagem

Toda mensagem precisa obrigatoriamente de um **tópico** de destino e do próprio **conteúdo**. Já são opcionais:

- **Partição**: pode ser escolhida manualmente ou seguir uma estratégia (por padrão, hash da chave ou rodízio);
- **Timestamp**: sempre existe um, mesmo que você não defina — pode ser a hora de criação ou a hora de log;
- **Message Key**: usada para decidir a partição, agrupar mensagens, fazer joins, etc.

Como os dados trafegam pela rede, eles precisam ser **serializados** — dá pra usar formatos como Avro pra isso.

## Producers

Um producer decide para qual partição mandar cada mensagem. Se a mensagem tem uma **key**, o Kafka aplica uma função hash nela, o que garante que mensagens com a mesma chave sempre caiam na mesma partição (enquanto o número de partições não mudar). Sem chave, a distribuição é feita por rodízio. Também é possível escrever sua própria lógica de particionamento.

Para poucas mensagens, um producer single-thread já resolve. Quando o volume cresce, vale escalar com múltiplas threads — só que, em vez de criar vários producers, o recomendado é usar um único producer "thread-safe" compartilhado entre as threads, enviando em paralelo.

Um producer/consumer pode ser criado de três formas: pelo **console** (linha de comando), por uma **API** (código) ou pelo **Kafka Connect** (conectores prontos).

## Consumers

Um consumer lê os dados de uma partição dentro de um broker, sempre respeitando a ordem daquela partição — mas, de novo, sem garantia de ordem entre partições diferentes. Um mesmo consumer pode ler de mais de uma partição ao mesmo tempo.

### Consumer Group

Consumers costumam ser organizados em **grupos** (consumer groups). Dentro de um grupo, cada partição é atribuída a apenas um consumer — então, se o grupo tiver mais consumers do que partições, alguns ficam ociosos, sem nada pra ler. Isso é o que permite escalar o consumo: adicionar mais consumers ao grupo (até o limite do número de partições) distribui a carga de leitura entre eles.

Dois grupos diferentes podem consumir o mesmo tópico de forma totalmente independente — cada grupo mantém seu próprio controle de offset, então um grupo não interfere no progresso do outro.

## Garantia de entrega

Existe sempre uma troca entre **performance** e **garantia de durabilidade**. Para registrar termos de busca de usuários, performance importa mais; para registrar transações financeiras, a garantia de que nada se perdeu é o que manda.

### Confirmações (acks)

O producer pode configurar o quanto ele exige de confirmação antes de considerar uma mensagem "enviada com sucesso" — isso afeta diretamente a performance:

- **acks=0**: o producer nem espera resposta, já considera enviado (mais rápido, menos seguro);
- **acks=1**: considera enviado assim que a partição Leader confirmar o recebimento;
- **acks=all**: só considera enviado quando todas as réplicas mínimas da lista ISR confirmarem (mais lento, mais seguro).

O Kafka também suporta **transações** (parecido com transações de banco de dados), mas exige que o tópico tenha fator de replicação de pelo menos 3 e um mínimo de réplicas sincronizadas de pelo menos 2.

### Os três modelos de entrega

- **Pelo menos uma vez** (o padrão): se o producer não recebe confirmação, ele reenvia. Isso significa que, em caso de falha na confirmação, o broker pode acabar recebendo (e entregando) a mesma mensagem mais de uma vez.
- **No máximo uma vez**: o producer nunca reenvia. Nunca vai duplicar, mas se algo falhar no meio do caminho, o consumer simplesmente não recebe aquela mensagem.
- **Exatamente uma vez**: o producer reenvia em caso de falha, mas o broker é inteligente o suficiente para perceber duplicatas e só persistir/entregar a mensagem uma única vez — o melhor dos dois mundos, ao custo de mais complexidade.

## Kafka Connect

Produzir ou consumir dados pode ser feito de três formas: pelo **console**, escrevendo código com uma **API**, ou usando o **Kafka Connect**.

O Connect funciona de forma declarativa: existem conectores prontos (para bancos de dados, sistemas de arquivo, filas, etc.) que só precisam ser instalados e configurados — sem precisar programar. Ele pode atuar tanto como producer (**Source**, trazendo dados de fora pra dentro do Kafka) quanto como consumer (**Sink**, levando dados do Kafka pra fora). Além disso, ele já vem com escalabilidade automática, tolerância a falha e balanceamento de carga, e pode rodar tanto sozinho (*standalone*) quanto em cluster (*distributed*).

Em modo cluster, cada conexão individual (seja Source ou Sink) roda como um **Connect Worker** — para aumentar a capacidade, basta adicionar mais workers. Um mesmo Connect pode rodar Source e Sink ao mesmo tempo.

O Connect também oferece transformações simples aplicadas em trânsito, as **SMTs** (Single Message Transforms), como `InsertField`, `ReplaceField`, `MaskField`, `ValueToKey`, `HoistField`, `ExtractField`, `SetSchemaMetadata`, `TimestampRouter`, `RegexRouter` e `Filter`.
