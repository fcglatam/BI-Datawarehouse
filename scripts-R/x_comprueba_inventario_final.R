# Diego Villamil, ACG 
# CDMX, 14 de febrero de 2018
# Feliz San Valentín. 

conn_pg <- dbConnect_pg()

inventory <- tbl(conn_pg, "xinventoryDaily") %>% 
  select(inventory_date, internal_id, selling_status, car_cost,
    inventory_days) %>% 
  collect() %>% 
  mutate(inventory_month = inventory_date %>% floor_date("month"))

daily <- inventory %>% 
  group_by(inventory_date) %>% 
  summarize(n_cars = n())


month_status <- inventory %>% 
  group_by(inventory_month, selling_status, inventory_date) %>% 
  summarize(n_cars = n()) %>% 
  summarize(n_avg = mean(n_cars))

gg_month_status <- month_status %>% 
  ggplot(aes(x = inventory_month, y = n_avg, fill = selling_status)) + 
  geom_col()

the_cut   <- c(0, 05, 10, 20, 30, 50, 1000)
the_label <- "[%d, %d)" %>% sprintf(the_cut %>% head(-1), the_cut %>% tail(-1))
  
month_bins <- inventory %>% 
  mutate(bin_10 = cut(inventory_days, breaks = the_cut, 
      labels = the_label, right = FALSE)) %>% 
  group_by(inventory_month, bin_10, inventory_date) %>% 
  summarize(n_cars = n()) %>% 
  summarize(n_avg = mean(n_cars))

gg_month_bins <- month_bins %>% 
  ggplot(aes(x = inventory_month, y = n_avg, fill = bin_10)) +
  geom_col()








