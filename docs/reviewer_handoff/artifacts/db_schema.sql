--
-- PostgreSQL database dump
--

\restrict 3Y1e2alqPB2eWEsXEdhlnjza8LCYQdngN5rE6v4rZ2RWDOcP4K9tfVsRDnWZPPn

-- Dumped from database version 16.11 (Debian 16.11-1.pgdg13+1)
-- Dumped by pg_dump version 16.11 (Debian 16.11-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: active_narratives; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.active_narratives (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    narrative_name character varying(255) NOT NULL,
    narrative_type character varying(50),
    affected_sectors text[],
    affected_tickers text[],
    sentiment double precision,
    strength double precision,
    evidence_count integer DEFAULT 0,
    first_detected_at timestamp with time zone DEFAULT now(),
    last_evidence_at timestamp with time zone DEFAULT now(),
    status character varying(20) DEFAULT 'active'::character varying,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.active_narratives OWNER TO alekfi_db;

--
-- Name: calibration_rules; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.calibration_rules (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    rule_type character varying(32),
    target_system character varying(32),
    rule_key character varying(128),
    rule_value jsonb,
    based_on_n_samples integer,
    confidence double precision,
    reasoning text,
    active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone,
    last_validated_at timestamp with time zone
);


ALTER TABLE public.calibration_rules OWNER TO openclaw;

--
-- Name: entities; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.entities (
    id uuid NOT NULL,
    name character varying(256) NOT NULL,
    entity_type character varying(64) NOT NULL,
    ticker character varying(32),
    related_tickers jsonb,
    metadata jsonb,
    created_at timestamp with time zone NOT NULL
);


ALTER TABLE public.entities OWNER TO openclaw;

--
-- Name: entity_observations; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.entity_observations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_name character varying(255) NOT NULL,
    ticker character varying(20),
    observation_type character varying(50) NOT NULL,
    sentiment double precision,
    magnitude double precision,
    source_platform character varying(100),
    source_post_id uuid,
    signal_id uuid,
    context text,
    created_at timestamp with time zone DEFAULT now(),
    metadata jsonb DEFAULT '{}'::jsonb
);


ALTER TABLE public.entity_observations OWNER TO alekfi_db;

--
-- Name: entity_state; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.entity_state (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    ticker character varying(20) NOT NULL,
    entity_name character varying(255),
    sentiment_7d double precision,
    sentiment_24h double precision,
    mention_count_24h integer DEFAULT 0,
    mention_count_7d integer DEFAULT 0,
    momentum double precision DEFAULT 0,
    last_signal_direction character varying(10),
    last_signal_at timestamp with time zone,
    last_signal_outcome character varying(20),
    signals_total integer DEFAULT 0,
    signals_correct integer DEFAULT 0,
    accuracy double precision,
    dominant_narrative text,
    updated_at timestamp with time zone DEFAULT now(),
    metadata jsonb DEFAULT '{}'::jsonb
);


ALTER TABLE public.entity_state OWNER TO alekfi_db;

--
-- Name: filtered_posts; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.filtered_posts (
    id uuid NOT NULL,
    raw_post_id uuid NOT NULL,
    relevance_score double precision NOT NULL,
    urgency character varying(16) NOT NULL,
    category character varying(128) NOT NULL,
    gatekeeper_reasoning text NOT NULL,
    filtered_at timestamp with time zone NOT NULL,
    analyzed boolean NOT NULL,
    relevance_tier character varying(32)
);


ALTER TABLE public.filtered_posts OWNER TO openclaw;

--
-- Name: pattern_performance; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.pattern_performance (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    signal_type character varying(50),
    source_platform character varying(100),
    ticker_sector character varying(50),
    market_regime character varying(50),
    direction character varying(10),
    time_of_day character varying(20),
    day_of_week character varying(10),
    total_signals integer DEFAULT 0,
    correct_signals integer DEFAULT 0,
    accuracy double precision,
    wilson_lower double precision,
    avg_return_1h double precision,
    avg_return_4h double precision,
    avg_return_24h double precision,
    avg_return_7d double precision,
    best_timeframe character varying(10),
    avg_conviction_when_correct double precision,
    avg_conviction_when_wrong double precision,
    last_signal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.pattern_performance OWNER TO alekfi_db;

--
-- Name: post_outcomes; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.post_outcomes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    post_id uuid,
    signal_id uuid,
    platform character varying(64),
    author character varying(256),
    content_hash character varying(64),
    entities jsonb,
    post_category character varying(64),
    gatekeeper_score double precision,
    brain_score double precision,
    prices_at_post jsonb,
    prices_at_1h jsonb,
    prices_at_4h jsonb,
    prices_at_24h jsonb,
    prices_at_7d jsonb,
    max_move_1h jsonb,
    max_move_24h jsonb,
    avg_sector_move_24h double precision,
    idiosyncratic_move double precision,
    was_predictive boolean,
    predictive_magnitude double precision,
    noise_score double precision,
    attribution_weight double precision DEFAULT 1.0,
    macro_contaminated boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_checked_at timestamp with time zone
);


ALTER TABLE public.post_outcomes OWNER TO openclaw;

--
-- Name: price_data; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.price_data (
    id uuid NOT NULL,
    symbol character varying(32) NOT NULL,
    price double precision NOT NULL,
    volume double precision,
    change_1h double precision,
    change_24h double precision,
    source character varying(64) NOT NULL,
    fetched_at timestamp with time zone NOT NULL
);


ALTER TABLE public.price_data OWNER TO openclaw;

--
-- Name: raw_posts; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.raw_posts (
    id uuid NOT NULL,
    platform character varying(64) NOT NULL,
    source_id character varying(512) NOT NULL,
    author character varying(256) NOT NULL,
    content text NOT NULL,
    url character varying(2048),
    tokens_mentioned jsonb,
    raw_metadata jsonb NOT NULL,
    scraped_at timestamp with time zone NOT NULL,
    processed boolean NOT NULL,
    source_published_at timestamp with time zone
);


ALTER TABLE public.raw_posts OWNER TO openclaw;

--
-- Name: sentiment_scores; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.sentiment_scores (
    id uuid NOT NULL,
    filtered_post_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    sentiment double precision NOT NULL,
    confidence double precision NOT NULL,
    urgency double precision NOT NULL,
    reasoning text NOT NULL,
    themes jsonb,
    mechanism text,
    scored_at timestamp with time zone NOT NULL
);


ALTER TABLE public.sentiment_scores OWNER TO openclaw;

--
-- Name: signal_attributions; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.signal_attributions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    signal_id uuid NOT NULL,
    was_correct boolean,
    return_achieved double precision,
    source_attribution jsonb,
    timing_attribution double precision,
    conviction_attribution jsonb,
    narrative_attribution jsonb,
    regime_attribution double precision,
    recommended_adjustments jsonb,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.signal_attributions OWNER TO alekfi_db;

--
-- Name: signal_feedback; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.signal_feedback (
    id uuid NOT NULL,
    signal_id uuid,
    was_accurate boolean NOT NULL,
    feedback_type character varying(32) NOT NULL,
    notes text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_feedback OWNER TO openclaw;

--
-- Name: signal_null_outcomes; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.signal_null_outcomes (
    id uuid NOT NULL,
    symbol character varying(32) NOT NULL,
    sample_ts timestamp with time zone NOT NULL,
    labels jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_null_outcomes OWNER TO alekfi_db;

--
-- Name: signal_outcomes; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.signal_outcomes (
    id uuid NOT NULL,
    signal_id uuid NOT NULL,
    instruments jsonb,
    predicted_direction character varying(16),
    intelligence_score integer,
    signal_type character varying(64),
    exclusivity_edge character varying(32),
    source_platforms jsonb,
    actionability character varying(32),
    price_at_creation jsonb,
    price_at_1h jsonb,
    price_at_4h jsonb,
    price_at_24h jsonb,
    price_at_48h jsonb,
    price_at_7d jsonb,
    correct_at_1h boolean,
    correct_at_4h boolean,
    correct_at_24h boolean,
    correct_at_48h boolean,
    correct_at_7d boolean,
    max_favorable_move_pct double precision,
    max_adverse_move_pct double precision,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    labels jsonb
);


ALTER TABLE public.signal_outcomes OWNER TO openclaw;

--
-- Name: signal_source_items; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.signal_source_items (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    signal_id uuid NOT NULL,
    raw_post_id uuid NOT NULL,
    contribution_weight double precision,
    platform character varying(64),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_source_items OWNER TO alekfi_db;

--
-- Name: signals; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.signals (
    id uuid NOT NULL,
    signal_type character varying(64) NOT NULL,
    affected_instruments jsonb NOT NULL,
    direction character varying(16) NOT NULL,
    conviction double precision NOT NULL,
    time_horizon character varying(64) NOT NULL,
    thesis text NOT NULL,
    source_posts jsonb,
    metadata jsonb,
    created_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone,
    sigma_score double precision,
    sigma_severity character varying(32),
    sigma_details jsonb,
    price_at_signal double precision,
    price_t1 double precision,
    price_t3 double precision,
    price_t7 double precision,
    price_t30 double precision,
    return_t1 double precision,
    return_t3 double precision,
    return_t7 double precision,
    return_t30 double precision,
    signal_accurate_t7 boolean,
    decay_validated_at timestamp with time zone,
    earnings_proximity integer,
    technical_signal character varying(32),
    price_target_upside double precision,
    macro_regime character varying(32),
    market_context jsonb,
    weight_score integer,
    weight_tier character varying(16),
    credibility_score integer,
    quality_score integer,
    influence_score integer,
    weight_data jsonb,
    source_event_time timestamp with time zone,
    time_to_signal_seconds integer,
    outcome character varying(16),
    signal_fingerprint character varying(16)
);


ALTER TABLE public.signals OWNER TO openclaw;

--
-- Name: signals_shadow; Type: TABLE; Schema: public; Owner: alekfi_db
--

CREATE TABLE public.signals_shadow (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    reason character varying(50) NOT NULL,
    payload jsonb NOT NULL
);


ALTER TABLE public.signals_shadow OWNER TO alekfi_db;

--
-- Name: source_credibility; Type: TABLE; Schema: public; Owner: openclaw
--

CREATE TABLE public.source_credibility (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_type character varying(32),
    platform character varying(64),
    author character varying(256),
    total_posts integer DEFAULT 0,
    predictive_posts integer DEFAULT 0,
    accuracy_rate double precision DEFAULT 0.0,
    avg_predictive_magnitude double precision DEFAULT 0.0,
    avg_noise_score double precision DEFAULT 0.5,
    accuracy_by_category jsonb DEFAULT '{}'::jsonb,
    recent_accuracy double precision DEFAULT 0.0,
    accuracy_trend character varying(16) DEFAULT 'neutral'::character varying,
    tier character varying(16) DEFAULT 'unranked'::character varying,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.source_credibility OWNER TO openclaw;

--
-- Name: active_narratives active_narratives_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.active_narratives
    ADD CONSTRAINT active_narratives_pkey PRIMARY KEY (id);


--
-- Name: calibration_rules calibration_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.calibration_rules
    ADD CONSTRAINT calibration_rules_pkey PRIMARY KEY (id);


--
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


--
-- Name: entity_observations entity_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.entity_observations
    ADD CONSTRAINT entity_observations_pkey PRIMARY KEY (id);


--
-- Name: entity_state entity_state_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.entity_state
    ADD CONSTRAINT entity_state_pkey PRIMARY KEY (id);


--
-- Name: entity_state entity_state_ticker_key; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.entity_state
    ADD CONSTRAINT entity_state_ticker_key UNIQUE (ticker);


--
-- Name: filtered_posts filtered_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.filtered_posts
    ADD CONSTRAINT filtered_posts_pkey PRIMARY KEY (id);


--
-- Name: pattern_performance pattern_performance_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.pattern_performance
    ADD CONSTRAINT pattern_performance_pkey PRIMARY KEY (id);


--
-- Name: post_outcomes post_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.post_outcomes
    ADD CONSTRAINT post_outcomes_pkey PRIMARY KEY (id);


--
-- Name: price_data price_data_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.price_data
    ADD CONSTRAINT price_data_pkey PRIMARY KEY (id);


--
-- Name: raw_posts raw_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.raw_posts
    ADD CONSTRAINT raw_posts_pkey PRIMARY KEY (id);


--
-- Name: raw_posts raw_posts_source_id_key; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.raw_posts
    ADD CONSTRAINT raw_posts_source_id_key UNIQUE (source_id);


--
-- Name: sentiment_scores sentiment_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.sentiment_scores
    ADD CONSTRAINT sentiment_scores_pkey PRIMARY KEY (id);


--
-- Name: signal_attributions signal_attributions_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.signal_attributions
    ADD CONSTRAINT signal_attributions_pkey PRIMARY KEY (id);


--
-- Name: signal_feedback signal_feedback_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.signal_feedback
    ADD CONSTRAINT signal_feedback_pkey PRIMARY KEY (id);


--
-- Name: signal_null_outcomes signal_null_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.signal_null_outcomes
    ADD CONSTRAINT signal_null_outcomes_pkey PRIMARY KEY (id);


--
-- Name: signal_outcomes signal_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.signal_outcomes
    ADD CONSTRAINT signal_outcomes_pkey PRIMARY KEY (id);


--
-- Name: signal_source_items signal_source_items_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.signal_source_items
    ADD CONSTRAINT signal_source_items_pkey PRIMARY KEY (id);


--
-- Name: signals signals_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.signals
    ADD CONSTRAINT signals_pkey PRIMARY KEY (id);


--
-- Name: signals_shadow signals_shadow_pkey; Type: CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.signals_shadow
    ADD CONSTRAINT signals_shadow_pkey PRIMARY KEY (id);


--
-- Name: source_credibility source_credibility_pkey; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.source_credibility
    ADD CONSTRAINT source_credibility_pkey PRIMARY KEY (id);


--
-- Name: entities uq_entity_name_type; Type: CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT uq_entity_name_type UNIQUE (name, entity_type);


--
-- Name: idx_calibration_rules_active; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_calibration_rules_active ON public.calibration_rules USING btree (active);


--
-- Name: idx_calibration_rules_key; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_calibration_rules_key ON public.calibration_rules USING btree (rule_key);


--
-- Name: idx_calibration_rules_type; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_calibration_rules_type ON public.calibration_rules USING btree (rule_type);


--
-- Name: idx_entity_obs_name; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_entity_obs_name ON public.entity_observations USING btree (entity_name, created_at DESC);


--
-- Name: idx_entity_obs_ticker; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_entity_obs_ticker ON public.entity_observations USING btree (ticker, created_at DESC);


--
-- Name: idx_entity_obs_type; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_entity_obs_type ON public.entity_observations USING btree (observation_type, created_at DESC);


--
-- Name: idx_entity_state_momentum; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_entity_state_momentum ON public.entity_state USING btree (momentum DESC);


--
-- Name: idx_entity_state_ticker; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_entity_state_ticker ON public.entity_state USING btree (ticker);


--
-- Name: idx_filtered_posts_analyzed; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_filtered_posts_analyzed ON public.filtered_posts USING btree (analyzed) WHERE (analyzed = false);


--
-- Name: idx_filtered_posts_filtered_at; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_filtered_posts_filtered_at ON public.filtered_posts USING btree (filtered_at DESC);


--
-- Name: idx_narratives_active; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_narratives_active ON public.active_narratives USING btree (status, strength DESC) WHERE ((status)::text = 'active'::text);


--
-- Name: idx_narratives_sectors; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_narratives_sectors ON public.active_narratives USING gin (affected_sectors);


--
-- Name: idx_pattern_perf_accuracy; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_pattern_perf_accuracy ON public.pattern_performance USING btree (wilson_lower DESC) WHERE (total_signals >= 10);


--
-- Name: idx_pattern_perf_combo; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE UNIQUE INDEX idx_pattern_perf_combo ON public.pattern_performance USING btree (COALESCE(signal_type, ''::character varying), COALESCE(source_platform, ''::character varying), COALESCE(ticker_sector, ''::character varying), COALESCE(market_regime, ''::character varying), COALESCE(direction, ''::character varying), COALESCE(time_of_day, ''::character varying), COALESCE(day_of_week, ''::character varying));


--
-- Name: idx_pattern_perf_lookup; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_pattern_perf_lookup ON public.pattern_performance USING btree (signal_type, source_platform, ticker_sector, market_regime, direction);


--
-- Name: idx_post_outcomes_category; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_category ON public.post_outcomes USING btree (post_category);


--
-- Name: idx_post_outcomes_created; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_created ON public.post_outcomes USING btree (created_at);


--
-- Name: idx_post_outcomes_pending_1h; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_pending_1h ON public.post_outcomes USING btree (created_at) WHERE (prices_at_1h IS NULL);


--
-- Name: idx_post_outcomes_pending_24h; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_pending_24h ON public.post_outcomes USING btree (created_at) WHERE (prices_at_24h IS NULL);


--
-- Name: idx_post_outcomes_pending_4h; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_pending_4h ON public.post_outcomes USING btree (created_at) WHERE (prices_at_4h IS NULL);


--
-- Name: idx_post_outcomes_pending_verdict; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_pending_verdict ON public.post_outcomes USING btree (created_at) WHERE ((was_predictive IS NULL) AND (prices_at_24h IS NOT NULL));


--
-- Name: idx_post_outcomes_platform; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_platform ON public.post_outcomes USING btree (platform);


--
-- Name: idx_post_outcomes_platform_predictive; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_platform_predictive ON public.post_outcomes USING btree (platform, was_predictive) WHERE (was_predictive IS NOT NULL);


--
-- Name: idx_post_outcomes_post_id; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_post_id ON public.post_outcomes USING btree (post_id);


--
-- Name: idx_post_outcomes_predictive; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_predictive ON public.post_outcomes USING btree (was_predictive);


--
-- Name: idx_post_outcomes_signal_id; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_post_outcomes_signal_id ON public.post_outcomes USING btree (signal_id);


--
-- Name: idx_raw_posts_scraped_at; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_raw_posts_scraped_at ON public.raw_posts USING btree (scraped_at DESC);


--
-- Name: idx_sentiment_scores_entity_id; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_sentiment_scores_entity_id ON public.sentiment_scores USING btree (entity_id);


--
-- Name: idx_sentiment_scores_filtered_post_id; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_sentiment_scores_filtered_post_id ON public.sentiment_scores USING btree (filtered_post_id);


--
-- Name: idx_signal_attributions_signal; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_signal_attributions_signal ON public.signal_attributions USING btree (signal_id);


--
-- Name: idx_signal_feedback_signal_id; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signal_feedback_signal_id ON public.signal_feedback USING btree (signal_id);


--
-- Name: idx_signal_null_outcomes_symbol_ts; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_signal_null_outcomes_symbol_ts ON public.signal_null_outcomes USING btree (symbol, sample_ts DESC);


--
-- Name: idx_signal_outcomes_signal_created; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signal_outcomes_signal_created ON public.signal_outcomes USING btree (signal_id, created_at);


--
-- Name: idx_signals_accuracy; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_accuracy ON public.signals USING btree (signal_type, direction) WHERE (signal_accurate_t7 IS NOT NULL);


--
-- Name: idx_signals_created_at; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_created_at ON public.signals USING btree (created_at DESC);


--
-- Name: idx_signals_decay_pending; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_decay_pending ON public.signals USING btree (created_at DESC) WHERE ((price_t30 IS NULL) AND (affected_instruments IS NOT NULL));


--
-- Name: idx_signals_macro_regime; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_macro_regime ON public.signals USING btree (macro_regime) WHERE (macro_regime IS NOT NULL);


--
-- Name: idx_signals_pending_1h; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_pending_1h ON public.signals USING btree (created_at) WHERE ((return_t1 IS NULL) AND (price_at_signal IS NOT NULL));


--
-- Name: idx_signals_pending_4h; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_pending_4h ON public.signals USING btree (created_at) WHERE ((return_t3 IS NULL) AND (price_at_signal IS NOT NULL));


--
-- Name: idx_signals_shadow_created; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_signals_shadow_created ON public.signals_shadow USING btree (created_at);


--
-- Name: idx_signals_shadow_reason; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX idx_signals_shadow_reason ON public.signals_shadow USING btree (reason);


--
-- Name: idx_signals_validated; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_validated ON public.signals USING btree (signal_accurate_t7) WHERE ((signal_accurate_t7 IS NOT NULL) AND (weight_data IS NOT NULL));


--
-- Name: idx_signals_weight; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX idx_signals_weight ON public.signals USING btree (weight_score DESC NULLS LAST);


--
-- Name: idx_source_cred_unique; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE UNIQUE INDEX idx_source_cred_unique ON public.source_credibility USING btree (source_type, platform, COALESCE(author, ''::character varying));


--
-- Name: ix_entities_ticker_type; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_entities_ticker_type ON public.entities USING btree (ticker, entity_type);


--
-- Name: ix_filtered_posts_urgency_category_analyzed; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_filtered_posts_urgency_category_analyzed ON public.filtered_posts USING btree (urgency, category, analyzed);


--
-- Name: ix_price_data_symbol_fetched; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_price_data_symbol_fetched ON public.price_data USING btree (symbol, fetched_at);


--
-- Name: ix_raw_posts_platform_scraped_processed; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_raw_posts_platform_scraped_processed ON public.raw_posts USING btree (platform, scraped_at, processed);


--
-- Name: ix_signal_source_items_filtered_post_id; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX ix_signal_source_items_filtered_post_id ON public.signal_source_items USING btree (raw_post_id);


--
-- Name: ix_signal_source_items_signal_id; Type: INDEX; Schema: public; Owner: alekfi_db
--

CREATE INDEX ix_signal_source_items_signal_id ON public.signal_source_items USING btree (signal_id);


--
-- Name: ix_signals_fingerprint; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_signals_fingerprint ON public.signals USING btree (signal_fingerprint) WHERE (signal_fingerprint IS NOT NULL);


--
-- Name: ix_signals_sigma_score; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_signals_sigma_score ON public.signals USING btree (sigma_score) WHERE (sigma_score IS NOT NULL);


--
-- Name: ix_signals_sigma_severity; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_signals_sigma_severity ON public.signals USING btree (sigma_severity) WHERE (sigma_severity IS NOT NULL);


--
-- Name: ix_signals_type_created_direction; Type: INDEX; Schema: public; Owner: openclaw
--

CREATE INDEX ix_signals_type_created_direction ON public.signals USING btree (signal_type, created_at, direction);


--
-- Name: filtered_posts filtered_posts_raw_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.filtered_posts
    ADD CONSTRAINT filtered_posts_raw_post_id_fkey FOREIGN KEY (raw_post_id) REFERENCES public.raw_posts(id);


--
-- Name: post_outcomes post_outcomes_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.post_outcomes
    ADD CONSTRAINT post_outcomes_post_id_fkey FOREIGN KEY (post_id) REFERENCES public.filtered_posts(id);


--
-- Name: post_outcomes post_outcomes_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.post_outcomes
    ADD CONSTRAINT post_outcomes_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.signals(id);


--
-- Name: sentiment_scores sentiment_scores_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.sentiment_scores
    ADD CONSTRAINT sentiment_scores_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: sentiment_scores sentiment_scores_filtered_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.sentiment_scores
    ADD CONSTRAINT sentiment_scores_filtered_post_id_fkey FOREIGN KEY (filtered_post_id) REFERENCES public.filtered_posts(id);


--
-- Name: signal_feedback signal_feedback_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.signal_feedback
    ADD CONSTRAINT signal_feedback_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.signals(id);


--
-- Name: signal_outcomes signal_outcomes_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: openclaw
--

ALTER TABLE ONLY public.signal_outcomes
    ADD CONSTRAINT signal_outcomes_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.signals(id) ON DELETE CASCADE;


--
-- Name: signal_source_items signal_source_items_raw_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.signal_source_items
    ADD CONSTRAINT signal_source_items_raw_post_id_fkey FOREIGN KEY (raw_post_id) REFERENCES public.raw_posts(id);


--
-- Name: signal_source_items signal_source_items_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: alekfi_db
--

ALTER TABLE ONLY public.signal_source_items
    ADD CONSTRAINT signal_source_items_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.signals(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 3Y1e2alqPB2eWEsXEdhlnjza8LCYQdngN5rE6v4rZ2RWDOcP4K9tfVsRDnWZPPn

