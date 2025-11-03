# Bibliotecas ----
library(arrow)     # leitura de arquivos .parquet
library(sf)        # operações de geometria
library(ggplot2)   # Fazer mapas
library(dplyr)     # manipulação de dados
library(sfarrow)   # leitura de parquet com geometria
library(geobr)     # Ler geometrias de municipios
library(gridExtra)
library(grid)
library(ggnewscale)
library(cowplot)
library(ggspatial)
library(tidyr)

# Definir diretorio de trabalho -----------------------------------------
setwd("D:/CEM_acessoSAN/")

dir_inputs <- "1-inputs" # arquivos de input
dir_parcial <- "2-parcial" # arquivos intermediarios
dir_outputs <- "3-outputs" # arquivos finais

# Listar municipios -----------------------------------------------------
municipios <- c(
  3550308,   # Sao Paulo
  2507507,   # Joao Pessoa
  3106200,   # Belo Horizonte
  4314902,   # Porto Alegre
  1721000,   # Palmas
  5300108,   # Brasilia
  5208707    # Goiania
)

for (mun in municipios) {

  caminho_hex <- file.path(dir_parcial, mun, "hex/hex_urbanizado.parquet")
  hex <- sfarrow::st_read_parquet(caminho_hex)
  
  caminho_g0 <- file.path(dir_parcial, mun, "rais/tempos/ttm_g0.rds")
  acesso_g0 <- readRDS(caminho_g0) %>%
    mutate(tempo_15 = ifelse(travel_time_p50 <= 15, 1, 0)) %>%
    mutate(tempo_15 = replace_na(tempo_15, 0)) %>% # MUITO IMPORTANTE: +de 60min era NA, agora 0!!!
    group_by(from_id) %>%
    summarise(n_g0_15min = sum(tempo_15, na.rm = TRUE), .groups = "drop")

  caminho_g4 <- file.path(dir_parcial, mun, "rais/tempos/ttm_g4.rds")
  acesso_g4 <- readRDS(caminho_g4) %>%
    mutate(tempo_15 = ifelse(travel_time_p50 <= 15, 1, 0)) %>%
    mutate(tempo_15 = replace_na(tempo_15, 0)) %>% # MUITO IMPORTANTE: +de 60min era NA, agora 0!!!
    group_by(from_id) %>%
    summarise(n_g4_15min = sum(tempo_15, na.rm = TRUE), .groups = "drop")
  
  hex_acesso <- hex %>%
    left_join(acesso_g0, by = c("h3_address" = "from_id")) %>%
    left_join(acesso_g4, by = c("h3_address" = "from_id")) %>%
    rename(from_id = h3_address) %>%
    mutate(
      rel_g0_g4 = case_when(
        is.na(n_g0_15min) ~ NA_real_,
        is.na(n_g4_15min) ~ NA_real_,
        n_g4_15min == 0 ~ NA_real_,             
        TRUE ~ n_g0_15min / n_g4_15min
      ),
      
      indicador_so_g0 = n_g0_15min > 0 & n_g4_15min == 0,
      indicador_so_g4 = n_g4_15min == 0 & n_g4_15min > 0,
      indicador_sem_acesso_g0_g4 = 
        (n_g0_15min == 0 | is.na(n_g0_15min)) & 
        (n_g4_15min == 0 | is.na(n_g4_15min))
    ) %>%
    select(from_id, n_g0_15min, n_g4_15min, rel_g0_g4,
           indicador_so_g0, indicador_so_g4, indicador_sem_acesso_g0_g4)
  
  sf::st_write(hex_acesso, paste0(mun, "_relacao_g0_g4.gpkg"))
  
}
