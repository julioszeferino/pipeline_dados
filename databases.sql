create database dw_pnad;

create table dim_cores(
	id_cor BIGINT not null unique,
	cor varchar(25) not null unique,
	data_criacao date not null,
	primary key (id_cor)
);

create table dim_regioes(
	regiao_id BIGINT not null,
	regiao_nome varchar(25) not null unique,
	data_criacao date not null,
	primary key(regiao_id)
);

create table dim_estados(
	id BIGINT not null,
	sigla char(2) not null,
	nome varchar(50) not null,
	regiao_id int not null,
	data_criacao date not null,
	primary key(id),
	constraint fk_dim_estados_regiao foreign key(regiao_id) references dim_regioes(regiao_id)
);

create table dim_tempo(
	id_calendario char(6) not null,
	ano int not null,
	trimestre int not null,
	primary key (id_calendario)
);

create table fato_pnads(
	id bigint not null,
	sk_tempo char(6) not null,
	sk_cor bigint not null,
	sk_estado bigint not null,
	qtd_pessoas int default 0,
	qtd_graduacao int default 0,
	qtd_ocupados int default 0,
	renda_media decimal(15, 2) default 0.00,
	horastrab_media decimal(15, 2) default 0.00,
	primary key(id),
	constraint fk_fato_pnads_tempo foreign key(sk_tempo) references dim_tempo(id_calendario),
	constraint fk_fato_pnads_cor foreign key(sk_cor) references dim_cores(id_cor),
	constraint fk_fato_pnads_estado foreign key(sk_estado) references dim_estados(id)
);
