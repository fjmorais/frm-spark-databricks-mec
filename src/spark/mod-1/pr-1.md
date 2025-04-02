# Instalação Local do Spark

## Windows

1. **Instalar Java**
   - Baixe e instale o [Java JDK](https://www.oracle.com/java/technologies/downloads/)
   - Configure a variável `JAVA_HOME`: 
     - Clique com o botão direito em "Este Computador" > "Propriedades" > "Configurações avançadas do sistema" > "Variáveis de ambiente"
     - Em "Variáveis do sistema", clique em "Novo" e adicione:
       - Nome da variável: `JAVA_HOME`
       - Valor da variável: `C:\Program Files\Java\jdk-11.0.XX` (caminho da sua instalação do JDK)

2. **Instalar Spark**
   - Baixe o [Apache Spark](https://spark.apache.org/downloads.html)
   - Extraia para `C:\spark`
   
3. **Verificar a versão do Spark instalada**
   - Abra o Explorador de Arquivos
   - Navegue até `C:\spark`
   - Procure o arquivo `RELEASE` ou verifique o nome da pasta se ela incluir a versão
   - Anote a versão instalada para configurar as variáveis de ambiente corretamente

4. **Configurar variáveis de ambiente**:
     - Clique com o botão direito em "Este Computador" > "Propriedades" > "Configurações avançadas do sistema" > "Variáveis de ambiente"
     - Em "Variáveis do sistema", clique em "Novo" e adicione:
       - Nome da variável: `SPARK_HOME`
       - Valor da variável: `C:\spark`
     - Encontre a variável "Path" e clique em "Editar"
     - Clique em "Novo" e adicione: `%SPARK_HOME%\bin`

3. **Instalar Hadoop Winutils** (necessário no Windows)
   - Baixe [winutils.exe](https://github.com/cdarlint/winutils) para sua versão do Hadoop
   - Crie pasta `C:\hadoop\bin` e mova o arquivo para lá
   - Configure:
     - Em "Variáveis do sistema", clique em "Novo" e adicione:
       - Nome da variável: `HADOOP_HOME`
       - Valor da variável: `C:\hadoop`
     - Encontre a variável "Path" e clique em "Editar"
     - Clique em "Novo" e adicione: `%HADOOP_HOME%\bin`

5. **Verificar instalação**
   - Abra prompt de comando e execute:
     ```
     spark-shell --version
     ```
   - Para iniciar o shell interativo do Spark:
     ```
     spark-shell
     ```

## macOS

1. **Instalar Homebrew** (se ainda não tiver)
   ```
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. **Instalar Java e Spark**
   ```
   brew install java
   brew install apache-spark
   ```

3. **Verificar a versão do Spark instalada**
   ```
   brew info apache-spark
   ```
   Anote a versão instalada (por exemplo, 3.4.1) para configurar as variáveis de ambiente.

4. **Configurar variáveis de ambiente**
   - Abra o terminal e digite:
     ```
     nano ~/.zshrc   # para macOS Catalina ou superior
     # ou
     nano ~/.bash_profile   # para versões mais antigas
     ```
   - Adicione as seguintes linhas ao arquivo:
     ```
     export JAVA_HOME=$(/usr/libexec/java_home -v 11)
     export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.X.X/libexec
     export PATH=$PATH:$SPARK_HOME/bin
     export PATH=$JAVA_HOME/bin:$PATH
     ```
     (Substitua 3.X.X pela versão do Spark instalada)
   - Salve o arquivo (Ctrl+O, Enter, Ctrl+X)
   - Atualize o terminal:
     ```
     source ~/.zshrc  # ou source ~/.bash_profile
     ```

5. **Verificar instalação**
   ```
   spark-shell --version
   ```
   E para iniciar o shell interativo do Spark:
   ```
   spark-shell
   ```

## Linux (Ubuntu/Debian)

1. **Instalar Java**
   ```
   sudo apt update
   sudo apt install default-jdk
   ```

2. **Instalar Spark**
   ```
   wget https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
   tar -xvzf spark-3.4.1-bin-hadoop3.tgz
   sudo mv spark-3.4.1-bin-hadoop3 /opt/spark
   ```

3. **Verificar a versão do Spark instalada**
   ```
   ls -l /opt/spark/
   # ou
   cat /opt/spark/RELEASE
   ```
   Anote a versão instalada para configurar as variáveis de ambiente corretamente.

4. **Configurar variáveis de ambiente**
   - Abra o terminal e digite:
     ```
     nano ~/.bashrc
     ```
   - Adicione as seguintes linhas ao arquivo:
     ```
     export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
     export SPARK_HOME=/opt/spark
     export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
     ```
     (Verifique o caminho exato do Java usando `ls /usr/lib/jvm/`)
   - Salve o arquivo (Ctrl+O, Enter, Ctrl+X)
   - Atualize o terminal:
     ```
     source ~/.bashrc
     ```

4. **Verificar instalação**
   ```
   spark-shell
   ```

## Explorando o Diretório de Instalação do Spark

Após a instalação, é importante conhecer a estrutura de diretórios do Spark:

1. **Estrutura básica**
   - `bin/`: Contém scripts executáveis como `spark-shell`, `spark-submit`, `pyspark`
   - `conf/`: Arquivos de configuração
   - `jars/`: Bibliotecas JAR do Spark
   - `python/`: Código fonte do PySpark
   - `examples/`: Exemplos de código para Spark

2. **Arquivos de configuração importantes**
   - No diretório `conf/`, você encontrará arquivos de template como:
     - `spark-defaults.conf.template`: Configurações padrão
     - `spark-env.sh.template`: Variáveis de ambiente
     - `log4j2.properties.template`: Configurações de log

   Para usar estes arquivos, remova o sufixo `.template` e faça suas modificações.

3. **Verificando a instalação**
   No Windows:
   ```
   dir %SPARK_HOME%
   ```

   No macOS/Linux:
   ```
   ls -la $SPARK_HOME
   ```

## Solução de Problemas Comuns

### Problemas com Java

1. **Java não encontrado ("Unable to locate a Java Runtime" ou similar)**
   - Verifique se o Java está instalado: `java -version`
   - Reinstale o Java se necessário:
     - **Windows**: Baixe do site oficial da Oracle
     - **macOS**: `brew install --cask temurin` ou `brew install --cask oracle-jdk`
     - **Linux**: `sudo apt install default-jdk` (Debian/Ubuntu) ou `sudo yum install java-11-openjdk` (RedHat/CentOS)
   
2. **Variável JAVA_HOME incorreta**
   - Verifique o valor atual:
     - **Windows**: `echo %JAVA_HOME%`
     - **macOS/Linux**: `echo $JAVA_HOME`
   - Encontre o caminho correto:
     - **Windows**: Procure a pasta de instalação (geralmente em `C:\Program Files\Java\jdk-xx.x.x`)
     - **macOS**: Execute `/usr/libexec/java_home`
     - **Linux**: Geralmente em `/usr/lib/jvm/java-xx-openjdk-amd64` ou similar
   - Atualize a variável conforme descrito na seção de instalação

### Problemas com o Spark

1. **Comando "spark-shell" não encontrado**
   - Verifique se o Spark está nas variáveis de ambiente:
     - **Windows**: `echo %SPARK_HOME%` e `echo %PATH%`
     - **macOS/Linux**: `echo $SPARK_HOME` e `echo $PATH`
   - Verifique se os binários existem:
     - **Windows**: `dir %SPARK_HOME%\bin`
     - **macOS/Linux**: `ls -la $SPARK_HOME/bin`
   - Recarregue as variáveis de ambiente:
     - **Windows**: Reinicie o prompt de comando
     - **macOS**: `source ~/.zshrc` ou `source ~/.bash_profile`
     - **Linux**: `source ~/.bashrc`

2. **Erro ao iniciar o Spark shell**
   - Verifique logs para detalhes do erro:
     - **Windows**: Procure no diretório `%SPARK_HOME%\logs`
     - **macOS/Linux**: Procure no diretório `$SPARK_HOME/logs`
   - Problemas comuns:
     - **Conflito de versões**: Certifique-se que a versão do Spark é compatível com a versão do Java
     - **Falta de memória**: Adicione `--driver-memory 2g` ao comando spark-shell

### Problemas específicos do Windows

1. **Erro com winutils.exe**
   - Mensagem: `Exception in thread "main" java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0`
   - Solução:
     - Verifique se `winutils.exe` está no local correto: `%HADOOP_HOME%\bin`
     - Baixe a versão correta do winutils.exe para sua versão do Hadoop
     - Execute o prompt de comando como administrador

### Problemas específicos do macOS

1. **Erro de permissão na pasta /tmp**
   - Mensagem: `Permission denied: user=xxx, access=WRITE, inode="/tmp/spark-xxx"`
   - Solução:
     - Altere as permissões: `chmod 1777 /tmp`
     - Ou defina um diretório temporário diferente: `export SPARK_LOCAL_DIRS=~/tmp`

2. **Erro com caminho do Spark após instalação via Homebrew**
   - Mensagem: `/opt/homebrew/bin/spark-shell: line XX: /usr/local/Cellar/apache-spark/X.X.X/libexec/bin/spark-submit: No such file or directory`
   - Solução:
     - O caminho real do Spark está em uma localização diferente. Execute:
       ```
       ls -la /opt/homebrew/Cellar/apache-spark/
       ```
     - Encontre a versão correta instalada (pode ser diferente de 3.5.5)
     - Atualize seu .zshrc ou .bash_profile com o caminho correto:
       ```
       export SPARK_HOME=/opt/homebrew/Cellar/apache-spark/X.X.X/libexec
       ```
     - Onde X.X.X é a versão que você encontrou
     - Recarregue seu perfil: `source ~/.zshrc`

### Problemas específicos do Linux

1. **Erro "Native library libnvcompiler not found"**
   - Ocorre em sistemas com GPU NVIDIA
   - Solução:
     - Adicione `--conf spark.rapids.sql.enabled=false` ao iniciar o spark-shell
     - Ou instale as bibliotecas CUDA apropriadas
