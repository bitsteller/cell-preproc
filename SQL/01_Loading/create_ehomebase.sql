-- Table: public.homebase

DROP TABLE IF EXISTS public.ehomebase CASCADE;

-- CreateEhomebase
-- Create the table for storing the home location of each user
CREATE TABLE public.ehomebase
(
  id serial,
  user_id varchar,
  antenna_id bigint,
  hits integer,
  geom geometry(Point,4326),
  CONSTRAINT ehomebase_pkey PRIMARY KEY (id)
);

CREATE INDEX ehomebaseantennaid_idx
  ON public.ehomebase
  USING btree
  (antenna_id);
CREATE INDEX ehomebaseuserid_idx
  ON public.ehomebase
  USING btree
  (user_id);

-- Index: public.ehomebaseantennaid_idx

DROP INDEX IF EXISTS public.ehomebaseantennaid_idx;

CREATE INDEX ehomebaseantennaid_idx
  ON public.ehomebase
  USING btree
  (antenna_id);

-- Index: public.ehomebaseuserid_idx

DROP INDEX  IF EXISTS public.ehomebaseuserid_idx;

CREATE INDEX ehomebaseuserid_idx
  ON public.ehomebase
  USING btree
  (user_id);
