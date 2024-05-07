
# Ambiente de teste utilizando docker.
 

## SOFTWARES NECESSÁRIOS
#### Para a criação e uso do ambiente vamos utilizar o git e o Docker 
   * Instalação do Docker Desktop no Windows [Docker Desktop](https://hub.docker.com/editions/community/docker-ce-desktop-windows) ou o docker no [Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/)
   *  [Instalação do git](https://git-scm.com/book/pt-br/v2/Come%C3%A7ando-Instalando-o-Git)
   

INICIANDO O AMBIENTE*

#### Em um terminal/DOS/PowerShell, realizar o clone do projeto no github.
          git clone https://github.com/fabiogjardim/datatest.git

#### Ao realizar o clone do repositório, o diretória datatest será criado em sua máquina local.
   
## COMO INICIR O AMBIENTE

  *No Windows abrir PowerShell, do Linux um terminal e acessar o diretório datatest*
  
### Para iniciar um ambiente com Data Lake e Spark

          docker-compose up -d # streaming-data-lake-house
