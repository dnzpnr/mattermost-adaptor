# Kaynak test portlama eşlemesi

Biçim: `kaynak test` → `ported test` veya `not ported: gerekçe`.

## `test_bridge_relay.py`

- `test_gecerli_mesaj_dogru_hesapla_roleler` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_post_lifecycle_librechat_ve_sqlite_sureleri_kimlikle_loglanir` → `test_listener.py::test_post_lifecycle_logs_have_identity_and_correlation`
- `test_cevap_rest_postu_post_ve_kanal_kimligiyle_olculur` → `test_transport.py::test_create_post_body_preserves_props_and_configurable_reply_key`
- `test_mcp_suresi_post_ve_kanal_kimligiyle_loglanir` → not ported: MCP adapter kapsamından çıkarıldı.
- `test_post_lifecycle_istenen_dallari_ayri_adlandirir` → `test_listener.py::test_post_lifecycle_logs_have_identity_and_correlation`
- `test_bildirim_karari_mcpye_dogrudan_gider_agent_atlanir` → not ported: çelişki ve MCP dalları kapsamdan çıkarıldı.
- `test_kaliba_uymayan_mesaj_agent_yolunda_kalir` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics` (agent yerine `MessageHandler`).
- `test_baska_tenantin_celiskisi_dogrudan_yolda_cozulemez` → not ported: tenant/çelişki dalı kapsamdan çıkarıldı.
- `test_zaten_cozulmus_celiski_ikinci_kez_cozulmez` → not ported: çelişki dalı kapsamdan çıkarıldı.
- `test_dogrudan_mcp_hatasi_kullaniciya_bildirilir` → not ported: MCP dalı kapsamdan çıkarıldı.
- `test_t7b_eslenmemis_kanal_sessiz_kalinir` → `test_listener.py::test_unmapped_conversation_is_silent_and_not_processed`
- `test_t7a_kendi_mesajina_tepki_verilmez` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_system_add_to_channel_ajana_gitmez_cursoru_ilerletir_ve_dali_loglar` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_system_alt_tipleri_ajana_gitmeden_tamamlanir` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_bos_type_normal_postu_etkilemez_role_gider` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_type_eksik_veya_none_ise_normal_post_role_gider` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_provizyonsuz_tenant_uyari_alir` → not ported: hesap provizyonu ve tenant kapsamdan çıkarıldı.
- `test_ayni_threadin_ikinci_mesaji_ayni_conversation_idyi_kullanir` → `test_normalize_state.py::test_default_session_is_thread_scoped_and_never_user_scoped`
- `test_aciklamali_ekli_post_once_islenir_sonra_ajana_gider` → `test_listener.py::test_file_only_post_reaches_generic_handler` (ingestion yerine `MessageHandler`).
- `test_aciklamasiz_ekli_post_ajana_gitmez` → `test_listener.py::test_file_only_post_reaches_generic_handler` (dosya postu ingestion yerine `MessageHandler`a verilir).
- `test_ingestion_hatasi_da_ajana_baglam_olarak_gecer` → not ported: ingestion kapsamdan çıkarıldı.
- `test_dosya_isleme_hatasi_kullaniciya_bildirilir` → not ported: ingestion kapsamdan çıkarıldı.
- `test_dosya_isleyici_yoksa_uyari_verir` → not ported: ingestion kapsamdan çıkarıldı.
- `test_bozuk_json_sessizce_yoksayilir` → `test_listener.py::test_invalid_websocket_json_is_silently_ignored`
- `test_replay_son_islenen_posttan_sonraki_mesajlari_normal_akista_isler` → `test_listener.py::test_replay_after_cursor_is_chronological_and_deduplicated`
- `test_cursor_websocketten_once_baslatilir_aradaki_mesaj_replay_edilir` → `test_listener.py::test_cursor_is_initialized_before_websocket_and_gap_is_replayed`
- `test_replay_botun_kendi_mesajini_atlar_ama_cursoru_ilerletir` → `test_listener.py::test_filters_and_dedupe_preserve_cursor_semantics`
- `test_ayni_post_websocket_ve_replay_ile_iki_kez_islenmez` → `test_listener.py::test_replay_after_cursor_is_chronological_and_deduplicated`
- `test_replay_ust_siniri_asimi_bariz_uyari_verir` → `test_listener.py::test_replay_overflow_processes_newest_one_hundred`
- `test_replay_rest_cagrilarinin_her_biri_ve_toplam_sure_olculur` → `test_listener.py::test_post_lifecycle_logs_have_identity_and_correlation` ve `test_listener.py::test_replay_overflow_processes_newest_one_hundred`
- `test_t1_role_blokluyken_olay_dongusu_serbest_kalir` → `test_listener.py::test_slow_handler_does_not_block_receiver_from_accepting_next_post`
- `test_t1_dosya_isleme_blokluyken_olay_dongusu_serbest_kalir` → `test_listener.py::test_slow_handler_does_not_block_receiver_from_accepting_next_post` (ingestion yerine sync `MessageHandler`).
- `test_t1_celiski_cevabi_blokluyken_olay_dongusu_serbest_kalir` → `test_listener.py::test_slow_handler_does_not_block_receiver_from_accepting_next_post` (MCP/REST dalı yerine sync `MessageHandler`).
- `test_t1_postu_tamamla_blokluyken_olay_dongusu_serbest_kalir` → `test_listener.py::test_full_queue_applies_backpressure_without_dropping`
- `test_t1_replay_rest_blokluyken_olay_dongusu_serbest_kalir` → `test_listener.py::test_hello_barrier_orders_replay_before_queued_live_posts`
- `test_t2_yavas_role_surerken_sonraki_post_gercek_recv_yolundan_okunur` → `test_listener.py::test_slow_handler_does_not_block_receiver_from_accepting_next_post`
- `test_t3_kuyruk_postlari_tek_worker_ile_sirayla_isler` → `test_listener.py::test_full_queue_applies_backpressure_without_dropping`
- `test_t4_worker_istisnasi_ozgun_nesnesiyle_disari_tasinir` → `test_listener.py::test_single_worker_keeps_order_and_handler_failure_is_original`
- `test_t5_kopusta_inflight_thread_bitmeden_yeni_worker_baslamaz` → `test_listener.py::test_disconnect_waits_for_inflight_handler_before_next_session`
- `test_t6_hello_oncesi_ve_replay_sirasinda_gelen_canli_postlar_replayden_sonra_islenir` → `test_listener.py::test_hello_barrier_orders_replay_before_queued_live_posts`
- `test_t7c_post_tamamlama_dis_yan_etkiden_sonra_yazilir` → `test_listener.py::test_processed_mark_is_after_successful_handler_side_effect`
- `test_t8_dolu_kuyruk_post_atmaz_backpressure_uygular` → `test_listener.py::test_full_queue_applies_backpressure_without_dropping`
- `test_t8_kuyruk_kapasitesi_env_ile_ayarlanir_ve_replay_sinirindan_kucuktur` → `test_transport.py::test_config_validates_token_numeric_values_and_queue_before_network`
- `test_t9_101_postluk_replay_tasmada_en_eski_postu_belgelenen_semantikle_atlar` → `test_listener.py::test_replay_overflow_processes_newest_one_hundred`
- `test_t10_canli_ve_replay_ayni_postu_yalniz_bir_kez_isler` → `test_listener.py::test_replay_after_cursor_is_chronological_and_deduplicated`
- `test_geri_al_komutu_ajana_gitmez_deterministik_uygulanir` → not ported: geri alma/MCP dalı kapsamdan çıkarıldı.
- `test_serbest_metin_geri_al_komutu_sayilmaz` → not ported: geri alma komut ayrıştırması kapsamdan çıkarıldı.
- `test_geri_alici_yoksa_acik_uyari_verilir` → not ported: geri alma/MCP dalı kapsamdan çıkarıldı.
- `test_geri_alma_hatasi_kullaniciya_bildirilir` → not ported: geri alma/MCP dalı kapsamdan çıkarıldı.
- `test_bot_cevabi_hangi_mesaja_ait_oldugunu_tasir` → `test_transport.py::test_create_post_body_preserves_props_and_configurable_reply_key`; istisna kuralı uyarınca `mindalert_cevap_post_id` açıkça verildi.
- `test_deterministik_yollarin_cevabi_da_isaretlenir` → not ported: deterministik MCP dalları kapsamdan çıkarıldı.

## `test_mattermost_bot_run.py`

- `test_mm_token_bosken_ag_cagrisindan_once_net_hata` → `test_transport.py::test_config_validates_token_numeric_values_and_queue_before_network`
- `test_agent_id_bosken_main_ag_cagrisindan_once_net_hata` → not ported: agent kimliği adapter kapsamından çıkarıldı.
- `test_trace_export_bind_varsayilani_guvenli_ve_env_ile_acilabilir` → not ported: trace export kapsamdan çıkarıldı.
- `test_celiski_surucusu_agent_id_olmadan_yalniz_mm_token_ile_kurulur` → not ported: çelişki cron'u kapsamdan çıkarıldı.
- `test_mattermost_request_timeout_sonlu_varsayilanla_surucuye_gecer` → `test_transport.py::test_config_validates_token_numeric_values_and_queue_before_network`
- `test_mattermost_request_timeout_env_ile_ayarlanir` → `test_transport.py::test_config_validates_token_numeric_values_and_queue_before_network`
- `test_watchdog_canli_penceresi_varsayilan_ve_env_ile_surucuye_gecer` → `test_transport.py::test_config_validates_token_numeric_values_and_queue_before_network`
- `test_lag_watchdog_esige_gore_loglar_veya_susar` → `test_transport.py::test_watchdog_threshold_and_receive_gap_emit_structured_events`
- `test_lag_watchdog_periyodik_canli_satirinda_pencerenin_azami_lagini_yazar` → `test_transport.py::test_watchdog_heartbeat_reports_window_maximum`
- `test_recv_boslugu_esik_asilinca_loglanir_altinda_susar` → `test_transport.py::test_watchdog_threshold_and_receive_gap_emit_structured_events`
- `test_websocket_kapanisinda_watchdog_iptal_edilir_ve_toplanir` → `test_transport.py::test_websocket_close_cancels_and_collects_watchdog`
- `test_websocket_kapaninca_surec_sonlanmaz_yeniden_baglanir` → `test_transport.py::test_permanent_auth_classification_and_reconnect_after_clean_close`
- `test_kalici_kimlik_hatasinda_net_hata_ile_cikar` → `test_transport.py::test_permanent_login_error_does_not_sleep`

## `test_tenant_resolver.py`

- `test_yeni_thread_kendi_idsini_kok_alir` → `test_normalize_state.py::test_root_and_reply_thread_extraction_is_preserved`
- `test_cevap_kokun_root_idsini_kullanir` → `test_normalize_state.py::test_root_and_reply_thread_extraction_is_preserved`
- `test_channel_id_eksikse_hata` → `test_normalize_state.py::test_root_and_reply_thread_extraction_is_preserved`
- `test_esleme_varsa_cozulur` → `test_normalize_state.py::test_default_session_is_thread_scoped_and_never_user_scoped` (tenant yerine thread resolver).
- `test_esleme_yoksa_fail_closed` → `test_normalize_state.py::test_unmapped_resolver_fails_closed_after_own_and_system_filters`
- `test_farkli_tenantlar_ayni_thread_idsiyle_carpismaz` → not ported: tenant kavramı adapter kapsamından çıkarıldı; kabul edilen kimlik aynı thread için bilerek aynıdır.

## `test_yonetim_db_migrasyon.py`

- `test_yeni_yonetim_db_ilk_andan_itibaren_0600_olusur` → `test_normalize_state.py::test_state_file_is_0600_and_uses_english_tables`
- `test_eski_semali_db_veri_kaybetmeden_yukseltilir` → not ported: standalone adapter için legacy yönetim DB migrasyonu belirtilmedi; İngilizce yeni şema test edildi.
- `test_migrasyon_reddedilmis_duplicate_adayi_bekleyene_tercih_eder` → not ported: çelişki migrasyonu kapsamdan çıkarıldı.
- `test_migrasyon_birden_fazla_kararli_duplicate_icinden_en_eskiyi_korur` → not ported: çelişki migrasyonu kapsamdan çıkarıldı.
- `test_eszamanli_duplicate_aday_inserti_engellenir` → not ported: çelişki tablosu kapsamdan çıkarıldı.
