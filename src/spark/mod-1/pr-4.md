# Configurando Spark com Docker: Guia Passo a Passo

Este guia mostra como configurar e executar Apache Spark usando Docker de forma simples e direta. Vamos usar a imagem bitnami/spark, que é leve e bem mantida.

## 1. Instalando o Docker

Caso ainda não tenha o Docker instalado:

### Windows
- Baixe e instale o [Docker Desktop](https://www.docker.com/products/docker-desktop)
- Durante a instalação, mantenha as opções padrão

### macOS
- Baixe e instale o [Docker Desktop](https://www.docker.com/products/docker-desktop)
- Siga as instruções do instalador

### Linux (Ubuntu)
```bash
sudo apt update
sudo apt install docker.io
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
# Faça logout e login para que as mudanças de grupo tenham efeito
```

## 2. Baixando a Imagem do Spark

Vamos baixar a imagem bitnami/spark:

```bash
docker pull bitnami/spark:latest
```

## 3. Preparando a Estrutura do Projeto

Vamos organizar nosso projeto com a seguinte estrutura:

```
frm-spark-databricks-mec/src/spark/mod-1/
├── data/               # Diretório para arquivos de dados (ex: mongodb_users.json)
└── scripts/            # Diretório para scripts Python (ex: pr-3-app.py)
```

Se os diretórios ainda não existirem, crie-os:

```bash
# Navegar até o diretório do módulo
cd frm-spark-databricks-mec/src/spark/mod-1

# Criar diretórios para dados e scripts
mkdir -p data scripts
```

## 4. Verificando Arquivos Existentes

Certifique-se que o arquivo de dados e o script estão nos lugares corretos:

```bash
# Verificar se o arquivo JSON está no diretório data
ls -la data/

# Verificar se o script pr-3-app.py está no diretório scripts
ls -la scripts/
```

Se necessário, copie o arquivo JSON para o diretório `data/` e o script para `scripts/`.

## 5. Iniciando o Container Spark

Vamos iniciar um container Spark que compartilha os diretórios do projeto:

```bash
docker run -d --name spark-container \
  -p 4040:4040 \
  -v "$(pwd)/data:/opt/bitnami/spark/data" \
  -v "$(pwd)/scripts:/opt/bitnami/spark/scripts" \
  bitnami/spark:latest
```

Este comando:
- Cria um container chamado `spark-container`
- Mapeia a porta 4040 (UI do Spark) para seu computador
- Monta o diretório `data` local no container em `/opt/bitnami/spark/data`
- Monta o diretório `scripts` local no container em `/opt/bitnami/spark/scripts`

## 6. Verificando se o Container está Rodando

```bash
docker ps
docker logs spark-container
```

Você deverá ver o container `spark-container` na lista.

## 7. Entrando no Container Spark

Para acessar o container:

```bash
docker exec -it spark-container bash
```

Agora você está dentro do container e pode executar comandos Spark diretamente.

## 8. Verificando os Diretórios Montados

Dentro do container, verifique se os diretórios foram montados corretamente:

```bash
# Verificar o diretório de dados
ls -la /opt/bitnami/spark/data

# Verificar o diretório de scripts
ls -la /opt/bitnami/spark/scripts
```

Você deve ver o arquivo JSON no diretório `data` e o script `pr-3-app.py` no diretório `scripts`.

## 9. Executando o Script via Spark-Submit

Dentro do container, execute:

```bash
cd /opt/bitnami/spark
spark-submit scripts/pr-3-app.py
```

## 10. Usando Parâmetros de Configuração do Spark

Para configurar memória e outras opções:

```bash
spark-submit \
  --master local[2] \
  --driver-memory 1g \
  --name "Aplicação Spark" \
  scripts/pr-3-app.py
```

## 11. Verificando a Interface Web do Spark

Durante a execução de uma aplicação, acesse:
- http://localhost:4040

Esta interface mostra detalhes sobre jobs, estágios, armazenamento e ambiente.

## 12. Parando e Reiniciando o Container

Para parar o container:
```bash
docker stop spark-container
```

Para reiniciar posteriormente:
```bash
docker start spark-container
```

Para remover o container:
```bash
docker rm spark-container
```

## Modificando o Script (Se necessário)

Se precisar modificar o script `pr-3-app.py`, você pode editar o arquivo localmente no diretório `scripts/`. Como ele está montado no container, as alterações serão refletidas imediatamente.

Alternativamente, você pode editar o arquivo diretamente no container usando um editor como o nano:

```bash
# No container
nano /opt/bitnami/spark/scripts/pr-3-app.py
```

## Dicas Úteis

1. **Verificar logs do container**:
   ```bash
   docker logs spark-container
   ```

2. **Executar spark-shell interativo**:
   ```bash
   docker exec -it spark-container spark-shell
   ```

3. **Executar pyspark interativo**:
   ```bash
   docker exec -it spark-container pyspark
   ```

4. **Ver arquivos dentro do container**:
   ```bash
   docker exec -it spark-container ls -la /opt/bitnami/spark/data
   ```

5. **Copiar arquivos adicionais para o container**:
   ```bash
   docker cp novo_arquivo.json spark-container:/opt/bitnami/spark/data/
   ```

---

Este guia permite que você execute aplicações Spark usando Docker de forma simples e direta. A abordagem com volumes compartilhados facilita o desenvolvimento, pois você pode editar os arquivos localmente e executá-los diretamente no container Spark.
