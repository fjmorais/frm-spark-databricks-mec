# Primeiros Passos com Spark-Submit: Executando Aplicações Spark em Produção

O `spark-submit` é a ferramenta oficial para executar aplicações Spark em ambientes de produção. Enquanto o Spark-Shell é ideal para exploração interativa, o `spark-submit` permite executar aplicações completas de forma programática e escalável.

## O que é o Spark-Submit?

O `spark-submit` é um utilitário que:
- Permite executar aplicações Spark como processos independentes
- Suporta vários modos de implantação (local, cluster)
- Gerencia recursos computacionais (memória, CPU)
- Funciona com código em Scala, Java, Python e R

## Comando Básico

```shell
spark-submit [opções] <arquivo-da-aplicação> [argumentos]
```

## Verificando as Opções Disponíveis

Para ver todas as opções disponíveis:

```shell
spark-submit --help
```

## Principais Opções

### Opções de Implantação

```shell
--deploy-mode <modo>
```

- **cluster**: o driver é executado em um dos nós do cluster
- **client** (padrão): o driver é executado localmente onde você está submetendo a aplicação

### Opções de Cluster

```shell
--master <destino>
```

- **yarn**: para clusters Hadoop YARN
- **mesos://host:port**: para clusters Mesos
- **spark://host:port**: para clusters Spark Standalone
- **kubernetes://host:port**: para clusters Kubernetes
- **local**: para execução local (desenvolvimento/testes)

## Configurando Recursos

| Opção | Descrição |
|-------|-----------|
| `--driver-memory` | Memória a ser usada pelo driver do Spark |
| `--driver-cores` | Núcleos de CPU a serem usados pelo driver |
| `--num-executors` | Número total de executores a serem usados |
| `--executor-memory` | Quantidade de memória para cada executor |
| `--executor-cores` | Número de núcleos de CPU para cada executor |
| `--total-executor-cores` | Número total de núcleos de executor |

## Exemplos Básicos

### Executando uma Aplicação Python Localmente

```shell
spark-submit \
  --master local[2] \
  primeiro_app.py
```

Este comando:
- Executa a aplicação `primeiro_app.py`
- Usa o modo local com 2 threads

### Executando uma Aplicação PySpark com Diferentes Configurações

Podemos usar algumas configurações simples para personalizar a execução:

```shell
spark-submit \
  --master local[2] \
  --driver-memory 1g \
  --name "App Exemplo MongoDB" \
  processar_usuarios.py
```

Este comando:
- Executa o script usando 2 threads
- Aloca 1GB de memória para o driver
- Define um nome amigável para a aplicação

### Outro Exemplo: Visualizando Campos Específicos

Vamos criar outro exemplo simples com PySpark para visualizar apenas alguns campos do arquivo:

```python
# Arquivo: visualizar_campos.py
from pyspark.sql import SparkSession

# Inicializa o Spark
spark = SparkSession.builder \
    .appName("Visualizando Campos") \
    .getOrCreate()

# Carrega os dados
usuarios = spark.read.json("data/entities/mongodb_users.json")

# Mostra apenas alguns campos específicos
print("Informações básicas dos usuários:")
usuarios.select("user_id", "email", "country", "city").show(5)

# Encerra a sessão
spark.stop()
```

Para executar:

```shell
spark-submit visualizar_campos.py
```

## Um Exemplo Prático

Vamos criar uma aplicação PySpark simples para processar dados de usuários e executá-la com `spark-submit`. Este exemplo utiliza o arquivo `data/entities/mongodb_users.json`.

### Crie um arquivo Python chamado `processar_usuarios.py`:

```python
from pyspark.sql import SparkSession

# Inicializa a SparkSession
spark = SparkSession.builder \
    .appName("Processamento de Usuários") \
    .getOrCreate()

# Carrega o arquivo JSON de usuários
print("Carregando dados de usuários...")
usuarios_df = spark.read.json("data/entities/mongodb_users.json")

# Exibe o número de registros
count = usuarios_df.count()
print(f"Total de usuários: {count}")

# Mostra os primeiros registros
print("\nPrimeiros registros:")
usuarios_df.show(3)

# Encerra a sessão
spark.stop()
```

### Execute com spark-submit:

```shell
spark-submit \
  --master local[*] \
  processar_usuarios.py
```

Este exemplo simples demonstra:
- Como iniciar uma SparkSession
- Como carregar um arquivo JSON
- Como contar registros
- Como visualizar alguns dados

## Curiosidades sobre o Spark-Submit

1. **Histórico de Aplicações**: Todas as aplicações enviadas com `spark-submit` aparecem na interface web do Spark History Server.

2. **Modo Silencioso**: Adicione `--verbose` para ver mais detalhes sobre o que está acontecendo durante a execução.

3. **Compatibilidade**: O `spark-submit` garante compatibilidade entre diferentes versões de dependências, evitando conflitos.

4. **Arquivos Adicionais**: Use `--files` para enviar arquivos de configuração ou dados para o aplicativo.

5. **Fila de Execução**: Em clusters YARN, use `--queue` para especificar a fila de recursos.

## Diferenças entre Spark-Shell e Spark-Submit

| Característica | Spark-Shell | Spark-Submit |
|----------------|-------------|--------------|
| Uso principal | Exploração interativa | Produção e batch |
| Estado | Mantém o estado entre comandos | Inicia do zero a cada execução |
| SparkContext | Pré-inicializado | Você deve criar em seu código |
| Efêmero | Sim (interação temporária) | Não (execução completa) |
| Monitoramento | Interface web temporária | Histórico persistente |

## Solução de Problemas Comuns

### Erro "não foi possível encontrar ou carregar a classe principal"

Verifique:
- O caminho correto do JAR
- O nome correto da classe principal
- Se todas as dependências estão incluídas

### Erros de memória

Use configurações como:
```shell
--driver-memory 4g --executor-memory 2g
```

### Aplicação fica presa no estado "ACCEPTED"

Verifique:
- Se há recursos suficientes no cluster
- A configuração da fila no YARN
- Os logs do driver

## Exercícios Práticos

1. Crie um script PySpark simples que leia o arquivo `mongodb_users.json` e apenas mostre os dados

2. Modifique o script para mostrar apenas emails e cidades dos usuários

3. Execute seu script com `spark-submit` usando diferentes configurações:
   - Use apenas 1 thread: `--master local[1]`
   - Use todos os cores disponíveis: `--master local[*]`

4. Abra o navegador em `http://localhost:4040` durante a execução para ver a interface web do Spark

## Próximos Passos

- Criar aplicações Spark mais complexas
- Explorar modos de execução em cluster
- Aprender a configurar recursos para diferentes tipos de cargas de trabalho
- Configurar logging e monitoramento para suas aplicações

---

**Nota**: Os exemplos desta aula assumem que o Apache Spark está corretamente instalado e configurado em seu ambiente.