# Primeiros Passos com Spark-Shell: Interface Interativa para Exploração de Dados

O Spark-Shell é uma ferramenta interativa que permite explorar os recursos do Apache Spark de forma rápida e iterativa. Nesta aula, vamos aprender os conceitos básicos para começar a usar o Spark-Shell efetivamente.

## O que é o Spark-Shell?

O Spark-Shell é um ambiente REPL (Read-Eval-Print Loop) que permite executar comandos Spark de forma interativa e ver os resultados instantaneamente. Existem duas versões principais:

- **spark-shell**: Usa Scala como linguagem de programação (padrão)
- **pyspark**: Usa Python como linguagem de programação

## Iniciando o Spark-Shell

Após a instalação do Apache Spark (conforme visto na aula anterior), você pode iniciar o Spark-Shell simplesmente executando um dos seguintes comandos no terminal:

```shell
# Para Scala
spark-shell

# Para Python
pyspark
```

### Opções Úteis para Inicialização

Você pode personalizar o comportamento do Spark-Shell através de diversas opções. Para ver todas as opções disponíveis:

```shell
spark-shell --help
pyspark --help
```

Alguns exemplos de opções úteis:

```shell
# Configurar memória para o driver
spark-shell --driver-memory 2g

# Configurar o número de cores a serem utilizados
spark-shell --master local[4]

# Iniciar com arquivos de configuração específicos
spark-shell --properties-file minha-config.conf
```

## Interface Web do Spark

Ao iniciar o Spark-Shell, uma interface web é automaticamente disponibilizada, geralmente em:

```
http://localhost:4040/
```

Esta interface permite:
- Visualizar jobs em execução
- Monitorar o uso de recursos
- Verificar planos de execução de consultas
- Analisar o armazenamento em cache

## Trabalhando com o Spark-Shell

Ao iniciar o Spark-Shell, você já terá disponíveis as seguintes variáveis:

- **sc**: SparkContext - ponto de entrada para funcionalidades de baixo nível
- **spark**: SparkSession - ponto de entrada para funcionalidades de alto nível (DataFrames e SQL)

### Exemplo Básico em Scala

Vamos ler e explorar o arquivo de status de pedidos:

```scala
// Leitura do arquivo JSON de status de pedidos
val statusDF = spark.read.json("data/entities/kafka_status.json")

// Contar o número de registros
println(s"Total de registros: ${statusDF.count()}")

// Visualizar o primeiro registro
println("Primeiro registro:")
println(statusDF.first())

// Mostrar os primeiros registros de forma formatada
println("Primeiros registros:")
statusDF.show(3)

// Ver a estrutura do schema
println("Estrutura do schema:")
statusDF.printSchema()
```

### Exemplo Básico em Python (PySpark)

O mesmo exemplo em Python:

```python
# Leitura do arquivo JSON de status de pedidos
statusDF = spark.read.json("data/entities/kafka_status.json")

# Contar o número de registros
print(f"Total de registros: {statusDF.count()}")

# Visualizar o primeiro registro
print("Primeiro registro:")
print(statusDF.first())

# Mostrar os primeiros registros de forma formatada
print("Primeiros registros:")
statusDF.show(3)

# Ver a estrutura do schema
print("Estrutura do schema:")
statusDF.printSchema()
```

## Explorando Dados com Spark-Shell

Agora que já conhecemos os comandos básicos, vamos explorar um pouco mais o arquivo `kafka_status.json`:

```scala
// Em Scala
// Explorando campos e estrutura aninhada
statusDF.select("status_id", "order_identifier", "status.status_name").show(5)

// Filtrando dados simples
statusDF.filter("status_id > 3").show()
```

Em Python:

```python
# Em PySpark
# Explorando campos e estrutura aninhada
statusDF.select("status_id", "order_identifier", "status.status_name").show(5)

# Filtrando dados simples
statusDF.filter("status_id > 3").show()
```

> **Nota para Live Coding**: Durante a demonstração, você pode experimentar com outros filtros e seleções para mostrar a flexibilidade do Spark-Shell.

## Executando SQL no Spark-Shell

O Spark-Shell também permite executar SQL sobre seus dados:

```scala
// Em Scala
// Registrar o DataFrame como uma tabela temporária
statusDF.createOrReplaceTempView("pedidos_status")

// Executar uma consulta SQL simples
spark.sql("""
  SELECT status_id, status.status_name as status
  FROM pedidos_status
  ORDER BY status_id
""").show()
```

Em Python:

```python
# Em PySpark
# Registrar o DataFrame como uma tabela temporária
statusDF.createOrReplaceTempView("pedidos_status")

// Executar uma consulta SQL simples
spark.sql("""
  SELECT status_id, status.status_name as status
  FROM pedidos_status
  ORDER BY status_id
""").show()
```

## Salvando o Histórico de Comandos

No Spark-Shell, você pode salvar o histórico de comandos que executou para uso futuro:

```scala
// Em Scala
:history > meu_historico.scala
```

Ou em Python:
```python
# No PySpark, use o comando do próprio Python
import readline
readline.write_history_file('meu_historico.py')
```

## Dicas Úteis

1. **Autocompletar**: Pressione TAB para ver opções disponíveis
2. **Histórico**: Use as setas para cima/baixo para navegar pelos comandos anteriores
3. **Abortar comando**: Pressione Ctrl+C para interromper um comando em execução
4. **Sair do Spark-Shell**: Digite `:quit` ou pressione Ctrl+D

## Próximos Passos

Nesta aula, vimos os comandos básicos para começar a usar o Spark-Shell. Nas próximas aulas, vamos explorar mais recursos do Apache Spark, incluindo:

- Transformações e ações em RDDs
- Operações avançadas com DataFrames
- Integração com fontes de dados externas
- Execução de aplicações Spark com spark-submit

## Exercícios Práticos

1. Inicie o Spark-Shell e carregue o arquivo `data/entities/kafka_status.json`
2. Conte o número total de registros no arquivo
3. Liste todos os diferentes status (`status_name`) presentes nos dados
4. Crie uma visualização temporária e use SQL para encontrar qual status ocorre com mais frequência
5. Explore a interface web do Spark (http://localhost:4040) e examine o plano de execução de uma de suas consultas

Lembre-se: a prática é fundamental para dominar o Spark-Shell. Experimente diferentes comandos e explore os dados disponíveis!

---

**Nota**: Os exemplos desta aula assumem que existe uma pasta `data/entities/` com arquivos de exemplo. Certifique-se de ajustar os caminhos conforme a estrutura do seu ambiente.