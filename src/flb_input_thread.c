/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_pipe.h>
#include <fluent-bit/flb_engine.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_mp.h>
#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_event_loop.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_scheduler.h>

#include <mpack/mpack.h>

#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_input_plugin.h>

#include <fluent-bit/flb_input_thread.h>

static void *worker(void *arg)
{
    struct flb_input_thread *it = arg;
    it->callback(it->write, it->data);
    fclose(it->write_file);
    return NULL;
}

static inline int handle_input_event(flb_pipefd_t fd, struct flb_config *config)
{
    int bytes;
    uint32_t type;
    uint32_t ins_id;
    uint64_t val = 0;

    bytes = flb_pipe_r(fd, &val, sizeof(val));
    if (bytes == -1) {
        flb_errno();
        return -1;
    }

    printf("2. pause val => %" PRIu64 "\n", val); fflush(stdout);

    /* Get type and key */
    type   = FLB_BITS_U64_HIGH(val);
    ins_id = FLB_BITS_U64_LOW(val);

    /* At the moment we only support events coming from an input coroutine */
    if (type == FLB_ENGINE_IN_CORO) {
        flb_input_coro_finished(config, ins_id);
    }
    else if (type == FLB_ENGINE_EV_INPUT) {
        printf("EV INPUT EVENT!\n");
        fflush(stdout);
    }
    else if (type == FLB_INPUT_THREAD_PARENT_TO_THREAD) {
        printf("MESSAGE FROM PARENT TO THREAD!\n"); fflush(stdout);
    }
    else {
        flb_error("[thread event loop] invalid event type %i for input handler",
                  type);
        exit(1);
        return -1;
    }

    return 0;
}

static int input_collector_fd(flb_pipefd_t fd, struct flb_input_instance *ins)
{
    struct mk_list *head;
    struct flb_input_collector *collector = NULL;
    struct flb_input_coro *input_coro;
    struct flb_config *config = ins->config;

    mk_list_foreach(head, &ins->collectors) {
        collector = mk_list_entry(head, struct flb_input_collector, _head_ins);
        if (collector->fd_event == fd) {
            break;
        }
        else if (collector->fd_timer == fd) {
            flb_utils_timer_consume(fd);
            break;
        }
        collector = NULL;
    }

    /* No matches */
    if (!collector) {
        return -1;
    }

    if (collector->running == FLB_FALSE) {
        return -1;
    }

    /* Trigger the collector callback */
    if (collector->instance->runs_in_coroutine) {
        input_coro = flb_input_coro_collect(collector, config);
        if (!input_coro) {
            return -1;
        }
        flb_input_coro_resume(input_coro);
    }
    else {
        collector->cb_collect(collector->instance, config,
                              collector->instance->context);
    }

    return 0;
}

static FLB_INLINE int engine_handle_event(flb_pipefd_t fd, int mask,
                                          struct flb_input_instance *ins,
                                          struct flb_config *config)
{
    int ret;

    if (mask & MK_EVENT_READ) {
        //if (config->ch_manager[0] == fd) {
            //ret = flb_engine_manager(fd, config);
            //if (ret == FLB_ENGINE_STOP || ret == FLB_ENGINE_EV_STOP) {
            //    return FLB_ENGINE_STOP;
            //}
        //}

        /* Try to match the file descriptor with a collector event */
        ret = input_collector_fd(fd, ins);
        if (ret != -1) {
            return ret;
        }
        else {
            printf("EVENT ON FD=%i\n", fd);
            // /exit(100);
        }

        /* Metrics exporter event ? */
#ifdef FLB_HAVE_METRICS
        //ret = flb_me_fd_event(fd, config->metrics);
        //if (ret != -1) {
        //    return ret;
        //}
#endif
    }

    return 0;
}


static void input_thread(void *data)
{
    int ret;
    int thread_id;
    char tmp[64];
    struct mk_event *event;
    struct flb_input_instance *ins;
    struct flb_bucket_queue *evl_bktq;
    struct flb_input_thread_instance *thi;
    struct flb_input_plugin *p;
    struct flb_sched *sched;
    struct flb_net_dns dns_ctx;

    /* Create the bucket queue (FLB_ENGINE_PRIORITY_COUNT priorities) */
    evl_bktq = flb_bucket_queue_create(FLB_ENGINE_PRIORITY_COUNT);
    if (!evl_bktq) {
        return;
    }

    thi = (struct flb_input_thread_instance *) data;
    ins = thi->ins;
    p = ins->p;

    flb_engine_evl_set(thi->evl);

    /* Create a scheduler context */
    sched = flb_sched_create(ins->config, thi->evl);
    if (!sched) {
        flb_plg_error(ins, "could not create thread scheduler");
        return;
    }
    flb_sched_ctx_set(sched);

    flb_coro_thread_init();

    flb_net_ctx_init(&dns_ctx);
    flb_net_dns_ctx_set(&dns_ctx);

    thread_id = thi->th->id;
    snprintf(tmp, sizeof(tmp) - 1, "flb-in-%s-w%i", ins->name, thread_id);
    mk_utils_worker_rename(tmp);

    /* invoke plugin 'init' callback */
    ret = p->cb_init(ins, ins->config, ins->data);
    if (ret == -1) {
        flb_error("failed initialize input %s", flb_input_name(ins));
        /* message the parent thread that this thread could not be initialized */
        flb_input_thread_init_fail(ins);
        return;
    }
    flb_plg_info(ins, "plugin has started (FIXME)");

    /* message the parent thread that this thread is ready to go */
    ret = flb_input_thread_is_ready(ins);
    if (ret == -1) {
        flb_error("failed initialize and deliver ready message '%s'",
                  flb_input_name(ins));
        return;
    }

    flb_info("[THREAD FIXME] signal_wait()");

    /*
     * Wait for parent thread to signal this thread so we can start collectors and
     * get into the event loop
     */
    ret = flb_input_thread_collectors_signal_wait(ins);
    if (ret == -1) {
        flb_error("could not retrieve collectors signal from parent thread on '%s'",
                  flb_input_name(ins));
        return;
    }

    flb_info("[THREAD FIXME] collectors_start()");

    ret = flb_input_thread_collectors_start(ins);

    while (1) {
        mk_event_wait(thi->evl);
        mk_event_foreach(event, thi->evl) {
            flb_info("[THREAD FIXME] event loop, event on fd=%i", event->fd);

        //flb_event_priority_live_foreach(event, evl_bktq, thi->evl, FLB_ENGINE_LOOP_MAX_ITER) {
            if (event->type == FLB_ENGINE_EV_CORE) {
                ret = engine_handle_event(event->fd, event->mask,
                                          ins, thi->config);
                if (ret == FLB_ENGINE_STOP) {
                }
                else if (ret == FLB_ENGINE_SHUTDOWN) {
                }
            }
            else if (event->type & FLB_ENGINE_EV_SCHED) {
                /* Event type registered by the Scheduler */
                flb_sched_event_handler(ins->config, event);
            }
            else if (event->type == FLB_ENGINE_EV_THREAD_ENGINE) {
                struct flb_output_flush *output_flush;

                /* Read the coroutine reference */
                ret = flb_pipe_r(event->fd, &output_flush, sizeof(struct flb_output_flush *));
                if (ret <= 0 || output_flush == 0) {
                    flb_errno();
                    continue;
                }

                /* Init coroutine */
                flb_coro_resume(output_flush->coro);
            }
            else if (event->type == FLB_ENGINE_EV_CUSTOM) {
                event->handler(event);
            }
            else if (event->type == FLB_ENGINE_EV_THREAD) {
                struct flb_upstream_conn *u_conn;
                struct flb_coro *co;

                /*
                 * Check if we have some co-routine associated to this event,
                 * if so, resume the co-routine
                 */
                u_conn = (struct flb_upstream_conn *) event;
                co = u_conn->coro;
                if (co) {
                    flb_trace("[engine] resuming coroutine=%p", co);
                    flb_coro_resume(co);
                }
            }
            else if (event->type == FLB_ENGINE_EV_INPUT) {
                printf("=> FLB_ENGINE_EV_INPUT; event on fd=%i\n", event->fd); fflush(stdout);
                handle_input_event(event->fd, ins->config);
            }
            else {
                printf("UNHANDLED!\n");
            }
        }
    }
}

static void input_thread_instance_destroy(struct flb_input_thread_instance *thi)
{
    if (thi->evl) {
        mk_event_loop_destroy(thi->evl);
    }

    /* ch_parent_events */
    if (thi->ch_parent_events[0] > 0) {
        mk_event_closesocket(thi->ch_parent_events[0]);
    }
    if (thi->ch_parent_events[1] > 0) {
        mk_event_closesocket(thi->ch_parent_events[1]);
    }

    /* ch_thread_events */
    if (thi->ch_thread_events[0] > 0) {
        mk_event_closesocket(thi->ch_thread_events[0]);
    }
    if (thi->ch_thread_events[1] > 0) {
        mk_event_closesocket(thi->ch_thread_events[1]);
    }

    flb_free(thi);
}

static struct flb_input_thread_instance *input_thread_instance_create(struct flb_input_instance *ins)
{
    int ret;
    struct flb_input_thread_instance *thi;

    /* context for thread */
    thi = flb_calloc(1, sizeof(struct flb_input_thread_instance));
    if (!thi) {
        flb_errno();
        return NULL;
    }
    thi->ins = ins;
    thi->config = ins->config;

    mk_list_init(&thi->input_coro_list);
    mk_list_init(&thi->input_coro_list_destroy);

    /* event loop */
    thi->evl = mk_event_loop_create(256);
    if (!thi->evl) {
        input_thread_instance_destroy(thi);
        return NULL;
    }

    /* channel to receive parent (engine) notifications */
    ret = mk_event_channel_create(thi->evl,
                                  &thi->ch_parent_events[0],
                                  &thi->ch_parent_events[1],
                                  thi);
    if (ret == -1) {
        flb_error("could not initialize parent channels for %s",
                  flb_input_name(ins));
        input_thread_instance_destroy(thi);
        return NULL;
    }

    printf("[pipe] parent_events: read=%i write=%i\n",
           thi->ch_parent_events[0], thi->ch_parent_events[1]);
    thi->event.type = FLB_ENGINE_EV_INPUT;

    /* create thread pool, just one worker */
    thi->tp = flb_tp_create(ins->config);
    if (!thi->tp) {
        flb_error("could not create thread pool on input instance '%s'",
                  flb_input_name(ins));
        input_thread_instance_destroy(thi);
        return NULL;
    }

    return thi;
}

/*
 * Signal the thread event loop to pause the running plugin instance. This function
 * must be called only from the main thread/pipeline.
 */
int flb_input_thread_pause_instance(struct flb_input_instance *ins)
{
    int ret;
    uint64_t val;
    struct flb_input_thread_instance *thi = ins->thi;

    /* compose message to pause the thread */
    val = FLB_BITS_U64_SET(FLB_INPUT_THREAD_PARENT_TO_THREAD,
                           FLB_INPUT_THREAD_PAUSE);


    uint32_t type;
    uint32_t ins_id;

    type   = FLB_BITS_U64_HIGH(val);
    ins_id = FLB_BITS_U64_LOW(val);

    assert(type == FLB_INPUT_THREAD_PARENT_TO_THREAD);
    assert(ins_id == FLB_INPUT_THREAD_PAUSE);
    printf("1. pause val => %" PRIu64 "\n", val); fflush(stdout);

    flb_info("thread pause instance , write to fd=%i (read=%i)",
             thi->ch_parent_events[1], thi->ch_parent_events[0]);
    ret = flb_pipe_w(thi->ch_parent_events[1], &val, sizeof(val));
    if (ret <= 0) {
        flb_errno();
        return -1;
    }

    printf("pausing through pipe %i (read), %i (write)\n",
           thi->ch_parent_events[0], thi->ch_parent_events[1]); fflush(stdout);
    return 0;
}

/* Initialize a plugin under a threaded context */
int flb_input_thread_instance_init(struct flb_config *config, struct flb_input_instance *ins)
{
    int ret;
    struct flb_tp_thread *th;
    struct flb_input_thread_instance *thi;

    thi = input_thread_instance_create(ins);
    if (!thi) {
        return -1;
    }
    ins->thi = thi;

    /* Spawn the thread */
    th = flb_tp_thread_create(thi->tp, input_thread, thi, config);
    if (!th) {
        flb_plg_error(ins, "could not register worker thread");
        input_thread_instance_destroy(thi);
        return -1;
    }
    thi->th = th;

    /* start the thread */
    ret = flb_tp_thread_start(thi->tp, thi->th);

    return ret;
}

int flb_input_thread_init_fail(struct flb_input_instance *ins)
{
    uint64_t fail = 0;
    size_t ret;
    struct flb_input_thread_instance *thi;

    thi = ins->thi;
    ret = flb_pipe_w(thi->ch_parent_events[1], &fail, sizeof(uint64_t));
    if (ret <= 0) {
        flb_errno();
        return -1;
    }

    return 0;
}

int flb_input_thread_is_ready(struct flb_input_instance *ins)
{
    uint64_t ready = 1;
    size_t ret;
    struct flb_input_thread_instance *thi;

    thi = ins->thi;
    ret = flb_pipe_w(thi->ch_parent_events[1], &ready, sizeof(uint64_t));
    if (ret <= 0) {
        flb_errno();
        return -1;
    }

    return 0;
}

/* Wait for an input thread instance to become ready, or a failure status */
int flb_input_thread_wait_until_is_ready(struct flb_input_instance *ins)
{
    uint64_t status = 0;
    size_t bytes;
    struct flb_input_thread_instance *thi;

    thi = ins->thi;

    bytes = read(thi->ch_parent_events[0], &status, sizeof(uint64_t));
    if (bytes <= 0) {
        flb_errno();
        return -1;
    }

    if (status == 0) {
        return -1;
    }

    return FLB_TRUE;
}


/*
 * Invoked from the main 'input' interface to signal the threaded plugin instance so
 * it can start the collectors.
 *
 * The child thread is just waiting for this message in a blocking state by using
 * the function flb_input_thread_collectors_signal_wait().
 */
int flb_input_thread_collectors_signal_start(struct flb_input_instance *ins)
{
    uint64_t ready = 1;
    size_t ret;
    struct flb_input_thread_instance *thi;

    flb_info("SIGNAL START");
    thi = ins->thi;
    ret = flb_pipe_w(thi->ch_parent_events[1], &ready, sizeof(uint64_t));
    if (ret <= 0) {
        flb_errno();
        return -1;
    }

    return 0;
}

int flb_input_thread_collectors_signal_wait(struct flb_input_instance *ins)
{
    uint64_t ready = 0;
    size_t bytes;
    struct flb_input_thread_instance *thi;

    flb_info("SIGNAL WAIT..");
    thi = ins->thi;

    bytes = read(thi->ch_parent_events[0], &ready, sizeof(uint64_t));
    if (bytes <= 0) {
        flb_errno();
        return -1;
    }

    return 0;
}

int flb_input_thread_collectors_start(struct flb_input_instance *ins)
{
    int ret;
    int c = 0;
    struct mk_list *head;
    struct flb_input_collector *coll;

    mk_list_foreach(head, &ins->collectors) {
        coll = mk_list_entry(head, struct flb_input_collector, _head_ins);
        ret = flb_input_collector_start(coll->id, ins);
        if (ret < 0) {
            return -1;
        }
    }

    return 0;
}

/* ------------------- OLD CODE START HERE ----------------------- */

int flb_input_thread_init(struct flb_input_thread *it, flb_input_thread_cb callback, void *data)
{
    flb_pipefd_t fd[2];
    int result;

    result = flb_pipe_create(fd);
    if (result) {
        flb_error("[input] failed to create pipe: %d", result);
        return -1;
    }

    it->read = fd[0];
    it->write = fd[1];
    it->data = data;
    it->callback = callback;
    it->bufpos = 0;
    it->write_file = fdopen(it->write, "ab");
    if (!it->write_file) {
        flb_errno();
        return -1;
    }

    it->exit = false;
    result = pthread_mutex_init(&it->mutex, NULL);
    if (result) {
        flb_error("[input] failed to initialize thread mutex: %d", result);
        return -1;
    }

    mpack_writer_init_stdfile(&it->writer, it->write_file, false);
    result = pthread_create(&it->thread, NULL, worker, it);
    if (result) {
        close(it->read);
        close(it->write);
        flb_error("[input] failed to create thread: %d", result);
        return -1;
    }

    return 0;
}

int flb_input_thread_collect(struct flb_input_instance *ins,
                             struct flb_config *config,
                             void *in_context)
{
    int object_count;
    size_t chunks_len;
    size_t remaining_bytes;
    struct flb_input_thread *it = in_context;

    int bytes_read = read(it->read,
                          it->buf + it->bufpos,
                          sizeof(it->buf) - it->bufpos);
    flb_plg_trace(ins, "input thread read() = %i", bytes_read);

    if (bytes_read == 0) {
        flb_plg_warn(ins, "end of file (read pipe closed by input thread)");
        return 0;
    }

    if (bytes_read <= 0) {
        flb_input_collector_pause(it->coll_fd, ins);
        flb_engine_exit(config);
        return -1;
    }
    it->bufpos += bytes_read;

    object_count = flb_mp_count_remaining(it->buf, it->bufpos, &remaining_bytes);
    if (!object_count) {
        // msgpack data is still not complete
        return 0;
    }

    chunks_len = it->bufpos - remaining_bytes;
    flb_input_chunk_append_raw(ins, NULL, 0, it->buf, chunks_len);
    memmove(it->buf, it->buf + chunks_len, remaining_bytes);
    it->bufpos = remaining_bytes;
    return 0;
}

int flb_input_thread_destroy(struct flb_input_thread *it, struct flb_input_instance *ins)
{
    int ret;
    flb_input_thread_exit(it, ins);
    ret = pthread_join(it->thread, NULL);
    mpack_writer_destroy(&it->writer);
    pthread_mutex_destroy(&it->mutex);
    return ret;
}

void flb_input_thread_exit(void *in_context, struct flb_input_instance *ins)
{
    struct flb_input_thread *it;

    if (!in_context) {
        flb_plg_warn(ins, "can't set exit flag, in_context not set");
        return;
    }

    it = in_context;
    pthread_mutex_lock(&it->mutex);
    it->exit = true;
    pthread_mutex_unlock(&it->mutex);
    flb_pipe_close(it->read);
}

bool flb_input_thread_exited(struct flb_input_thread *it)
{
    bool ret;
    pthread_mutex_lock(&it->mutex);
    ret = it->exit;
    pthread_mutex_unlock(&it->mutex);
    return ret;
}
