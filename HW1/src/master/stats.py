def print_master_stats(sessions, kv_store, fault_handler, scheduler, clock, logger):
    SEP = "=" * 90
    DIV = "-" * 90

    logger.raw(SEP)
    logger.raw("FINAL STATISTICS")
    logger.raw(SEP)

    total_success = 0
    total_fail = 0
    total_reject = 0
    total_dispatched = 0
    total_wait = 0.0
    total_proc = 0.0
    processed_count = 0

    for s in sessions:
        st = s.stats
        total_success += st.success
        total_fail += st.fail
        total_reject += st.reject
        total_dispatched += st.dispatched
        total_wait += st.total_wait_time
        total_proc += st.total_proc_time
        processed_count += st.success + st.fail

        logger.raw(
            f"Worker{s.worker_id}: dispatched={st.dispatched} "
            f"success={st.success} fail={st.fail} reject={st.reject}"
        )

    avg_wait = total_wait / processed_count if processed_count > 0 else 0.0

    logger.raw(DIV)
    logger.raw(f"[1] Total tasks processed    : {total_success + total_fail}")
    logger.raw(f"[2] Total SUCCESS            : {total_success}")
    logger.raw(f"    Total FAIL               : {total_fail}")
    logger.raw(f"    Total REJECT             : {total_reject}")
    logger.raw(f"[3] Avg wait time            : {avg_wait:.2f} sec")
    logger.raw(f"[4] P2P load-balance events  : {scheduler.total_p2p_events}")
    logger.raw(f"[5] Fault re-assignments     : {fault_handler.total_requeues}")
    logger.raw(f"[6] Total elapsed time       : {clock.now():.2f} sec (System Clock)")
    logger.raw(f"    KV store committed       : {len(kv_store)}/{scheduler.num_kv}")
    logger.raw(SEP)
