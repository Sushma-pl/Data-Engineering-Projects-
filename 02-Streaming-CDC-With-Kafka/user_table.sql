create table public.users (
    id serial primary key,
    name text,
    emai text
);

Alter table public.users Replica Identity FULL ;

create publication dbz_publication for table public.users;