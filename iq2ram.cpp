//
// Copyright 2010-2011,2014 Ettus Research LLC
// Copyright 2018 Ettus Research, a National Instruments Company
//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Modified for triggered recording with signal detection

#include <uhd/exception.hpp>
#include <uhd/types/tune_request.hpp>
#include <uhd/usrp/multi_usrp.hpp>
#include <uhd/utils/safe_main.hpp>
#include <uhd/utils/thread.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <complex>
#include <csignal>
#include <fstream>
#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <unistd.h>

namespace po = boost::program_options;

// Generate RFC3339 timestamp with milliseconds
std::string get_timestamp_ms() {
    auto now = std::chrono::system_clock::now();
    auto now_t = std::chrono::system_clock::to_time_t(now);
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    std::tm* now_tm = std::gmtime(&now_t);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H-%M-%S", now_tm);
    return std::string(buf) + "." + std::to_string(now_ms.count()) + "Z";
}

// Generate filename from parameters
std::string generate_filename(const std::string& output_dir, const std::string& hostname,
    const std::string& datetime, double freq, double rate, double duration,
    double gain, const std::string& desc) {
    return str(boost::format("%s/iq_capture_%s_%s_%.0fMHz_%.1fMsps_%.1fs_%.0fdB_%s.dat")
        % output_dir % hostname % datetime % (freq/1e6) % (rate/1e6) % duration % gain % desc);
}

// Global state
static bool stop_signal_called = false;
static std::atomic<bool> triggered{false};
static std::atomic<int> detection_count{0};
static std::atomic<bool> enter_pressed{false};

// Double buffer synchronization
static std::mutex buf_mutex;
static std::condition_variable buf_ready;
static std::atomic<bool> detection_thread_running{true};

void sig_int_handler(int)
{
    stop_signal_called = true;
    detection_thread_running = false;
    enter_pressed = true;  // Also unblock wait mode
    buf_ready.notify_all();
}

// Thread function to wait for Enter key
void wait_for_enter()
{
    std::cin.get();
    enter_pressed = true;
}

// Compute mean power in dB for complex samples (matches Python implementation)
// Normalizes SC16 by 32768, computes |z|^2 / 50, returns 10*log10(mean)
template <typename samp_type>
double compute_mean_power_db(const samp_type* samples, size_t count)
{
    double sum = 0.0;
    const double norm = 32768.0;
    const double impedance = 50.0;

    for (size_t i = 0; i < count; i++) {
        double re = static_cast<double>(samples[i].real()) / norm;
        double im = static_cast<double>(samples[i].imag()) / norm;
        double mag_sq = re * re + im * im;  // |z|^2
        sum += mag_sq / impedance;
    }

    double mean_power = sum / static_cast<double>(count);
    return 10.0 * std::log10(mean_power + 1e-20);  // 10*log10 for power
}

// Top N tracker for highest power values
static std::vector<double> top_powers;
static const size_t TOP_N = 10;

void update_top_powers(double power_db) {
    if (top_powers.size() < TOP_N) {
        top_powers.push_back(power_db);
        std::sort(top_powers.begin(), top_powers.end(), std::greater<double>());
    } else if (power_db > top_powers.back()) {
        top_powers.back() = power_db;
        std::sort(top_powers.begin(), top_powers.end(), std::greater<double>());
    }
}

void print_top_powers() {
    if (top_powers.empty()) return;
    std::cout << "\nTop " << top_powers.size() << " highest power readings:" << std::endl;
    for (size_t i = 0; i < top_powers.size(); i++) {
        std::cout << "  " << (i + 1) << ". " << top_powers[i] << " dB" << std::endl;
    }
}

// Detection thread function
template <typename samp_type>
void detection_thread_fn(
    std::vector<samp_type>* process_buf,
    size_t* process_count,
    double threshold,
    int hyst)
{
    while (detection_thread_running && !triggered) {
        std::unique_lock<std::mutex> lock(buf_mutex);
        buf_ready.wait(lock, [&]{
            return !detection_thread_running || triggered || *process_count > 0;
        });

        if (!detection_thread_running || triggered) break;
        if (*process_count == 0) continue;

        // Compute mean power in dB
        double avg_db = compute_mean_power_db(process_buf->data(), *process_count);
        *process_count = 0;  // Mark as processed

        // Track top powers
        update_top_powers(avg_db);

        // Threshold check with hysteresis
        if (avg_db > threshold) {
            int count = ++detection_count;
            std::cout << "\ravg=" << avg_db << " dB [" << count << "/" << hyst << "] ABOVE    " << std::flush;
            if (count >= hyst) {
                triggered = true;
                std::cout << "\nTRIGGERED!" << std::endl;
            }
        } else {
            detection_count = 0;
            std::cout << "\ravg=" << avg_db << " dB [0/" << hyst << "]         " << std::flush;
        }
    }
}

template <typename samp_type>
void recv_to_file_triggered(
    uhd::usrp::multi_usrp::sptr usrp,
    const std::string& cpu_format,
    const std::string& wire_format,
    const size_t& channel,
    std::string file,
    size_t samps_per_buff,
    double time_requested,
    double threshold,
    int hyst,
    double detect_dur,
    double pre_trig_time,
    bool null,
    bool continue_on_bad_packet,
    const std::string& output_dir,
    const std::string& hostname,
    double freq,
    double gain,
    const std::string& file_desc,
    int skip_first,
    int start_retries,
    int capture_count,
    double dead_time)
{
    // Create receive streamer
    uhd::stream_args_t stream_args(cpu_format, wire_format);
    std::vector<size_t> channel_nums;
    channel_nums.push_back(channel);
    stream_args.channels = channel_nums;
    uhd::rx_streamer::sptr rx_stream = usrp->get_rx_stream(stream_args);

    uhd::rx_metadata_t md;
    double sample_rate = usrp->get_rx_rate(channel);

    // Calculate buffer sizes
    size_t detect_samples = static_cast<size_t>(detect_dur * sample_rate);
    size_t pre_trig_samples = static_cast<size_t>(pre_trig_time * sample_rate);
    size_t recording_samples = static_cast<size_t>(time_requested * sample_rate);

    std::cout << "Detection window: " << detect_samples << " samples (" << detect_dur << "s)" << std::endl;
    std::cout << "Pre-trigger buffer: " << pre_trig_samples << " samples (" << pre_trig_time << "s)" << std::endl;
    std::cout << "Recording duration: " << recording_samples << " samples (" << time_requested << "s)" << std::endl;

    // Double buffers for detection
    std::vector<samp_type> detect_buf_a(detect_samples);
    std::vector<samp_type> detect_buf_b(detect_samples);
    std::vector<samp_type>* active_buf = &detect_buf_a;
    std::vector<samp_type>* process_buf = &detect_buf_b;
    size_t active_buf_idx = 0;
    size_t process_count = 0;

    // Circular pre-trigger buffer
    std::vector<samp_type> pre_trig_buffer(pre_trig_samples);
    size_t pre_trig_write_idx = 0;
    bool pre_trig_full = false;

    // Pre-allocate RAM buffer for post-trigger recording only
    std::vector<samp_type> ram_buffer;

    std::cout << "Pre-allocating RAM buffer for " << recording_samples << " samples ("
              << (recording_samples * sizeof(samp_type) / 1e6) << " MB)..." << std::endl;
    ram_buffer.resize(recording_samples);
    std::cout << "RAM buffer allocated." << std::endl;

    // Start detection thread
    std::thread detector(detection_thread_fn<samp_type>,
                        process_buf, &process_count, threshold, hyst);

    // Temp buffer for skip/warmup
    std::vector<samp_type> temp_buf(samps_per_buff);

    // Start continuous streaming
    uhd::stream_cmd_t stream_cmd(uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS);
    stream_cmd.stream_now = true;
    rx_stream->issue_stream_cmd(stream_cmd);

    // Skip first recv() calls (warm-up)
    for (int i = 0; i < skip_first; i++) {
        rx_stream->recv(temp_buf.data(), samps_per_buff, md, 3.0, false);
    }

    // Determine if infinite mode (count=0)
    bool infinite_mode = (capture_count == 0);
    int total_captures = infinite_mode ? 999999 : capture_count;

    // === CAPTURE LOOP ===
    for (int capture_num = 1; capture_num <= total_captures && !stop_signal_called; capture_num++) {
        // Reset state for this capture
        triggered = false;
        detection_count = 0;
        detection_thread_running = true;
        top_powers.clear();

        // Reset pre-trigger buffer state
        pre_trig_write_idx = 0;
        pre_trig_full = false;

        // Reset detection buffer state
        active_buf = &detect_buf_a;
        process_buf = &detect_buf_b;
        active_buf_idx = 0;
        process_count = 0;

        if (infinite_mode) {
            std::cout << "\n=== Capture " << capture_num << " (infinite mode) ===" << std::endl;
        } else {
            std::cout << "\n=== Capture " << capture_num << "/" << capture_count << " ===" << std::endl;
        }
        std::cout << "Waiting for trigger (threshold=" << threshold << ", hyst=" << hyst << ")..." << std::endl;

        // Start detection thread for this capture
        std::thread detector(detection_thread_fn<samp_type>,
                            process_buf, &process_count, threshold, hyst);

        bool overflow_message = true;

        // === PRE-TRIGGER PHASE ===
        while (!stop_signal_called && !triggered) {
            size_t num_rx_samps = rx_stream->recv(
                &(*active_buf)[active_buf_idx],
                std::min(samps_per_buff, detect_samples - active_buf_idx),
                md, 3.0, false);

            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) {
                std::cout << "Timeout while streaming" << std::endl;
                break;
            }
            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
                if (overflow_message) {
                    overflow_message = false;
                    std::cerr << "Overflow during detection phase" << std::endl;
                }
                continue;
            }
            if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
                if (continue_on_bad_packet) continue;
                else throw std::runtime_error(md.strerror());
            }

            // Copy to circular pre-trigger buffer using bulk memcpy
            const samp_type* src = &(*active_buf)[active_buf_idx];
            size_t remaining = num_rx_samps;
            while (remaining > 0) {
                size_t space_to_end = pre_trig_samples - pre_trig_write_idx;
                size_t to_copy = std::min(remaining, space_to_end);
                std::memcpy(&pre_trig_buffer[pre_trig_write_idx], src, to_copy * sizeof(samp_type));
                src += to_copy;
                remaining -= to_copy;
                pre_trig_write_idx += to_copy;
                if (pre_trig_write_idx >= pre_trig_samples) {
                    pre_trig_write_idx = 0;
                    pre_trig_full = true;
                }
            }

            active_buf_idx += num_rx_samps;

            // Detection buffer full - swap and signal detection thread
            if (active_buf_idx >= detect_samples) {
                {
                    std::lock_guard<std::mutex> lock(buf_mutex);
                    std::swap(active_buf, process_buf);
                    process_count = detect_samples;
                }
                buf_ready.notify_one();
                active_buf_idx = 0;
            }
        }

        // Stop detection thread
        detection_thread_running = false;
        buf_ready.notify_all();
        detector.join();

        // Print top power readings
        print_top_powers();

        if (!triggered) {
            std::cout << "\nStopped without trigger." << std::endl;
            break;  // Exit capture loop
        }

        // === POST-TRIGGER PHASE ===
        // Retry logic at start of recording
        bool had_overflow = false;
        int start_attempts = 0;
        std::string recording_start_time;
        size_t num_total_samps = 0;

        while (start_attempts < start_retries && !stop_signal_called) {
            start_attempts++;
            num_total_samps = 0;

            size_t num_rx_samps = rx_stream->recv(
                ram_buffer.data(), samps_per_buff, md, 3.0, false);

            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
                std::cout << "Overflow at recording start, retrying (" << start_attempts << "/" << start_retries << ")" << std::endl;
                continue;
            }

            // Clean start - capture timestamp now
            recording_start_time = get_timestamp_ms();
            num_total_samps = num_rx_samps;
            break;
        }

        if (start_attempts >= start_retries && num_total_samps == 0) {
            std::cerr << "Failed to start recording cleanly after " << start_retries << " attempts" << std::endl;
            break;  // Exit capture loop
        }

        // Generate filename for this capture (always auto-generate in multi-capture mode)
        std::string capture_file = file;
        if (capture_file.empty() && !null) {
            capture_file = generate_filename(output_dir, hostname, recording_start_time, freq,
                usrp->get_rx_rate(channel), time_requested, gain, file_desc);
            std::cout << "Output file: " << capture_file << std::endl;
        }

        std::cout << "Recording..." << std::endl;

        while (!stop_signal_called && num_total_samps < recording_samples) {
            size_t remaining = recording_samples - num_total_samps;
            size_t to_recv = std::min(samps_per_buff, remaining);

            size_t num_rx_samps = rx_stream->recv(
                &ram_buffer[num_total_samps],
                to_recv,
                md, 3.0, false);

            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) break;
            if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
                if (!had_overflow) {
                    had_overflow = true;
                    std::cerr << "Overflow during recording!" << std::endl;
                }
                continue;
            }
            if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
                if (continue_on_bad_packet) continue;
                else throw std::runtime_error(md.strerror());
            }

            num_total_samps += num_rx_samps;
        }

        std::cout << "\nRecording complete. Total samples: " << num_total_samps << std::endl;

        // Add OVF to filename if overflow occurred
        if (had_overflow && !capture_file.empty()) {
            size_t dot_pos = capture_file.rfind(".dat");
            if (dot_pos != std::string::npos) {
                capture_file.insert(dot_pos, "_OVF");
            }
            std::cout << "Warning: Overflow occurred, file renamed to: " << capture_file << std::endl;
        }

        // Write to file - stitch pre-trigger + recording buffers
        if (!null) {
            size_t pre_trig_count = pre_trig_full ? pre_trig_samples : pre_trig_write_idx;
            size_t total_written = pre_trig_count + num_total_samps;

            std::cout << "Writing to file: " << pre_trig_count << " pre-trigger + "
                      << num_total_samps << " recorded = " << total_written << " samples" << std::endl;

            const auto write_start = std::chrono::steady_clock::now();

            std::ofstream outfile(capture_file.c_str(), std::ofstream::binary);
            if (!outfile.is_open()) {
                throw std::runtime_error("Failed to open output file");
            }

            // Write pre-trigger buffer (unwrap circular buffer)
            if (pre_trig_full) {
                // Buffer wrapped - write from write_idx to end, then start to write_idx
                outfile.write(reinterpret_cast<const char*>(&pre_trig_buffer[pre_trig_write_idx]),
                              (pre_trig_samples - pre_trig_write_idx) * sizeof(samp_type));
                outfile.write(reinterpret_cast<const char*>(&pre_trig_buffer[0]),
                              pre_trig_write_idx * sizeof(samp_type));
            } else {
                // Buffer not full - write from start to write_idx
                outfile.write(reinterpret_cast<const char*>(&pre_trig_buffer[0]),
                              pre_trig_write_idx * sizeof(samp_type));
            }

            // Write post-trigger recording
            outfile.write(reinterpret_cast<const char*>(ram_buffer.data()),
                          num_total_samps * sizeof(samp_type));
            outfile.close();

            const auto write_stop = std::chrono::steady_clock::now();
            double write_duration = std::chrono::duration<double>(write_stop - write_start).count();
            std::cout << "File write completed in " << write_duration << " seconds" << std::endl;
        }

        if (stop_signal_called) break;

        // Dead time - discard samples before next capture
        if (dead_time > 0 && capture_num < total_captures) {
            size_t dead_samples = static_cast<size_t>(dead_time * sample_rate);
            size_t discarded = 0;
            std::cout << "Dead time: discarding " << dead_time << "s of samples..." << std::endl;
            while (discarded < dead_samples && !stop_signal_called) {
                size_t to_discard = std::min(samps_per_buff, dead_samples - discarded);
                size_t num_rx = rx_stream->recv(temp_buf.data(), to_discard, md, 3.0, false);
                if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) break;
                discarded += num_rx;
            }
        }
    }

    // Stop streaming after all captures complete
    stream_cmd.stream_mode = uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS;
    rx_stream->issue_stream_cmd(stream_cmd);
}

// Immediate recording (no trigger) - record directly to RAM for duration
template <typename samp_type>
void recv_to_file_immediate(
    uhd::usrp::multi_usrp::sptr usrp,
    const std::string& cpu_format,
    const std::string& wire_format,
    const size_t& channel,
    std::string file,
    size_t samps_per_buff,
    double time_requested,
    bool null,
    bool continue_on_bad_packet,
    const std::string& output_dir,
    const std::string& hostname,
    double freq,
    double gain,
    const std::string& file_desc,
    int skip_first,
    int start_retries,
    bool wait_mode)
{
    // Create receive streamer
    uhd::stream_args_t stream_args(cpu_format, wire_format);
    std::vector<size_t> channel_nums;
    channel_nums.push_back(channel);
    stream_args.channels = channel_nums;
    uhd::rx_streamer::sptr rx_stream = usrp->get_rx_stream(stream_args);

    uhd::rx_metadata_t md;
    double sample_rate = usrp->get_rx_rate(channel);
    size_t recording_samples = static_cast<size_t>(time_requested * sample_rate);

    // Pre-allocate RAM buffer
    std::vector<samp_type> ram_buffer;
    std::cout << "Allocating RAM buffer for " << recording_samples << " samples ("
              << (recording_samples * sizeof(samp_type) / 1e6) << " MB)..." << std::endl;
    ram_buffer.resize(recording_samples);
    std::cout << "RAM buffer allocated." << std::endl;

    // Temp buffer for skip/warmup
    std::vector<samp_type> temp_buf(samps_per_buff);

    // Start continuous streaming
    uhd::stream_cmd_t stream_cmd(uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS);
    stream_cmd.stream_now = true;
    rx_stream->issue_stream_cmd(stream_cmd);

    // Skip first recv() calls (warm-up)
    for (int i = 0; i < skip_first; i++) {
        rx_stream->recv(temp_buf.data(), samps_per_buff, md, 3.0, false);
    }

    // Wait mode - discard samples until Enter is pressed
    if (wait_mode) {
        std::cout << "\nPress Enter to start recording..." << std::endl;
        enter_pressed = false;
        std::thread enter_thread(wait_for_enter);

        while (!enter_pressed && !stop_signal_called) {
            rx_stream->recv(temp_buf.data(), samps_per_buff, md, 3.0, false);
        }

        enter_thread.join();

        if (stop_signal_called) {
            std::cout << "\nAborted." << std::endl;
            stream_cmd.stream_mode = uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS;
            rx_stream->issue_stream_cmd(stream_cmd);
            return;
        }
    }

    // Retry logic at start
    bool had_overflow = false;
    int start_attempts = 0;
    std::string recording_start_time;
    size_t num_total_samps = 0;

    while (start_attempts < start_retries && !stop_signal_called) {
        start_attempts++;
        num_total_samps = 0;

        size_t num_rx_samps = rx_stream->recv(
            ram_buffer.data(), samps_per_buff, md, 3.0, false);

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
            std::cout << "Overflow at start, retrying (" << start_attempts << "/" << start_retries << ")" << std::endl;
            continue;
        }

        // Clean start - capture timestamp now
        recording_start_time = get_timestamp_ms();
        num_total_samps = num_rx_samps;
        break;
    }

    if (start_attempts >= start_retries && num_total_samps == 0) {
        std::cerr << "Failed to start cleanly after " << start_retries << " attempts" << std::endl;
        stream_cmd.stream_mode = uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS;
        rx_stream->issue_stream_cmd(stream_cmd);
        return;
    }

    // Generate filename if auto-filename mode (file is empty)
    if (file.empty() && !null) {
        file = generate_filename(output_dir, hostname, recording_start_time, freq,
            sample_rate, time_requested, gain, file_desc);
        std::cout << "Output file: " << file << std::endl;
    }

    std::cout << "Recording for " << time_requested << " seconds..." << std::endl;

    while (!stop_signal_called && num_total_samps < recording_samples) {
        size_t remaining = recording_samples - num_total_samps;
        size_t to_recv = std::min(samps_per_buff, remaining);

        size_t num_rx_samps = rx_stream->recv(
            &ram_buffer[num_total_samps],
            to_recv,
            md, 3.0, false);

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) break;
        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
            if (!had_overflow) {
                had_overflow = true;
                std::cerr << "Overflow during recording!" << std::endl;
            }
            continue;
        }
        if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
            if (continue_on_bad_packet) continue;
            else throw std::runtime_error(md.strerror());
        }

        num_total_samps += num_rx_samps;
    }

    // Stop streaming
    stream_cmd.stream_mode = uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS;
    rx_stream->issue_stream_cmd(stream_cmd);

    std::cout << "\nRecording complete. Total samples: " << num_total_samps << std::endl;

    // Add OVF to filename if overflow occurred
    if (had_overflow && !file.empty()) {
        size_t dot_pos = file.rfind(".dat");
        if (dot_pos != std::string::npos) {
            file.insert(dot_pos, "_OVF");
        }
        std::cout << "Warning: Overflow occurred, file renamed to: " << file << std::endl;
    }

    // Write to file
    if (!null && num_total_samps > 0) {
        std::cout << "Writing " << num_total_samps << " samples to file..." << std::endl;
        const auto write_start = std::chrono::steady_clock::now();

        std::ofstream outfile(file.c_str(), std::ofstream::binary);
        if (!outfile.is_open()) {
            throw std::runtime_error("Failed to open output file");
        }
        outfile.write(reinterpret_cast<const char*>(ram_buffer.data()),
                      num_total_samps * sizeof(samp_type));
        outfile.close();

        const auto write_stop = std::chrono::steady_clock::now();
        double write_duration = std::chrono::duration<double>(write_stop - write_start).count();
        std::cout << "File write completed in " << write_duration << " seconds" << std::endl;
    }
}

typedef std::function<uhd::sensor_value_t(const std::string&)> get_sensor_fn_t;

bool check_locked_sensor(std::vector<std::string> sensor_names,
    const char* sensor_name,
    get_sensor_fn_t get_sensor_fn,
    double setup_time)
{
    if (std::find(sensor_names.begin(), sensor_names.end(), sensor_name)
        == sensor_names.end())
        return false;

    auto setup_timeout = std::chrono::steady_clock::now()
                         + std::chrono::milliseconds(int64_t(setup_time * 1000));
    bool lock_detected = false;

    std::cout << boost::format("Waiting for \"%s\": ") % sensor_name;
    std::cout.flush();

    while (true) {
        if (lock_detected && (std::chrono::steady_clock::now() > setup_timeout)) {
            std::cout << " locked." << std::endl;
            break;
        }
        if (get_sensor_fn(sensor_name).to_bool()) {
            std::cout << "+";
            std::cout.flush();
            lock_detected = true;
        } else {
            if (std::chrono::steady_clock::now() > setup_timeout) {
                std::cout << std::endl;
                throw std::runtime_error(
                    str(boost::format("timed out waiting for lock on sensor \"%s\"")
                        % sensor_name));
            }
            std::cout << "_";
            std::cout.flush();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << std::endl;
    return true;
}

int UHD_SAFE_MAIN(int argc, char* argv[])
{
    // Variables to be set by program options
    std::string args, file, type, ant, subdev, ref, wirefmt, file_desc, output_dir;
    size_t channel, spb;
    double rate, freq, gain, bw, total_time, setup_time, lo_offset;
    double threshold, detect_dur, pre_trig_time, dead_time;
    int hyst, skip_first, start_retries, capture_count;

    // Setup program options
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help", "help message")
        ("args", po::value<std::string>(&args)->default_value(""), "multi uhd device address args")
        ("file", po::value<std::string>(&file), "output file (auto-generated if not specified)")
        ("desc", po::value<std::string>(&file_desc), "description tag for auto-filename (required if --file not specified)")
        ("dir", po::value<std::string>(&output_dir)->default_value("."), "output directory for auto-filename")
        ("type", po::value<std::string>(&type)->default_value("short"), "sample type: double, float, or short")
        ("duration", po::value<double>(&total_time)->default_value(5.0), "recording duration after trigger (seconds)")
        ("spb", po::value<size_t>(&spb)->default_value(100000), "samples per buffer")
        ("rate", po::value<double>(&rate)->default_value(1e6), "sample rate (Hz)")
        ("freq", po::value<double>(&freq)->default_value(0.0), "RF center frequency (Hz)")
        ("lo-offset", po::value<double>(&lo_offset)->default_value(0.0), "LO offset (Hz)")
        ("gain", po::value<double>(&gain), "RF gain (dB)")
        ("ant", po::value<std::string>(&ant), "antenna selection")
        ("subdev", po::value<std::string>(&subdev), "subdevice specification")
        ("channel", po::value<size_t>(&channel)->default_value(0), "channel to use")
        ("bw", po::value<double>(&bw), "analog frontend filter bandwidth (Hz)")
        ("ref", po::value<std::string>(&ref)->default_value("internal"), "reference source")
        ("wirefmt", po::value<std::string>(&wirefmt)->default_value("sc16"), "wire format")
        ("setup", po::value<double>(&setup_time)->default_value(1.0), "setup time (seconds)")
        ("null", "run without writing to file")
        ("continue", "don't abort on bad packet")
        ("skip-lo", "skip LO lock check")
        // Trigger options
        ("trig", po::value<double>(&threshold), "trigger threshold (dB)")
        ("hyst", po::value<int>(&hyst)->default_value(3), "hysteresis count for trigger")
        ("detect-dur", po::value<double>(&detect_dur)->default_value(0.5), "detection window duration (seconds)")
        ("pre-trig", po::value<double>(&pre_trig_time)->default_value(1.0), "pre-trigger buffer (seconds)")
        // Overflow handling
        ("skip-first", po::value<int>(&skip_first)->default_value(1), "skip first N recv() calls (warm-up)")
        ("start-retries", po::value<int>(&start_retries)->default_value(3), "max retries at start on overflow")
        // Wait mode
        ("wait", "wait for Enter key before recording (non-trigger mode only)")
        // Multi-capture
        ("count", po::value<int>(&capture_count)->default_value(1), "number of trigger captures (1-16, 0=infinite)")
        ("dead-time", po::value<double>(&dead_time)->default_value(60.0), "dead time between captures in seconds")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << "UHD RX Samples to File with Trigger" << std::endl;
        std::cout << desc << std::endl;
        return ~0;
    }

    bool null = vm.count("null") > 0;
    bool continue_on_bad_packet = vm.count("continue") > 0;
    bool trig_enabled = vm.count("trig") > 0;
    bool wait_mode = vm.count("wait") > 0;
    bool auto_filename = !vm.count("file") && !null;

    // Validate auto-filename requirements
    if (auto_filename && !vm.count("desc")) {
        std::cerr << "Error: --desc required when --file not specified" << std::endl;
        return ~0;
    }

    // Validate capture count (0=infinite, 1-16 valid)
    if (capture_count < 0 || capture_count > 16) {
        std::cerr << "Error: --count must be 0 (infinite) or 1-16" << std::endl;
        return ~0;
    }

    // Get hostname for auto-filename
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        strncpy(hostname, "unknown", sizeof(hostname));
    }
    double gain_val = vm.count("gain") ? gain : 0.0;

    // Create USRP device
    std::cout << std::endl;
    std::cout << boost::format("Creating the usrp device with: %s...") % args << std::endl;
    uhd::usrp::multi_usrp::sptr usrp = uhd::usrp::multi_usrp::make(args);

    // Lock mboard clocks
    if (vm.count("ref")) {
        usrp->set_clock_source(ref);
    }

    if (vm.count("subdev"))
        usrp->set_rx_subdev_spec(subdev);

    std::cout << boost::format("Using Device: %s") % usrp->get_pp_string() << std::endl;

    // Set sample rate
    if (rate <= 0.0) {
        std::cerr << "Please specify a valid sample rate" << std::endl;
        return ~0;
    }
    std::cout << boost::format("Setting RX Rate: %f Msps...") % (rate / 1e6) << std::endl;
    usrp->set_rx_rate(rate, channel);
    std::cout << boost::format("Actual RX Rate: %f Msps...") % (usrp->get_rx_rate(channel) / 1e6) << std::endl;

    // Set center frequency
    if (vm.count("freq")) {
        std::cout << boost::format("Setting RX Freq: %f MHz...") % (freq / 1e6) << std::endl;
        uhd::tune_request_t tune_request(freq, lo_offset);
        usrp->set_rx_freq(tune_request, channel);
        std::cout << boost::format("Actual RX Freq: %f MHz...") % (usrp->get_rx_freq(channel) / 1e6) << std::endl;
    }

    // Set RF gain
    if (vm.count("gain")) {
        std::cout << boost::format("Setting RX Gain: %f dB...") % gain << std::endl;
        usrp->set_rx_gain(gain, channel);
        std::cout << boost::format("Actual RX Gain: %f dB...") % usrp->get_rx_gain(channel) << std::endl;
    }

    // Set IF filter bandwidth
    if (vm.count("bw")) {
        std::cout << boost::format("Setting RX Bandwidth: %f MHz...") % (bw / 1e6) << std::endl;
        usrp->set_rx_bandwidth(bw, channel);
        std::cout << boost::format("Actual RX Bandwidth: %f MHz...") % (usrp->get_rx_bandwidth(channel) / 1e6) << std::endl;
    }

    // Set antenna
    if (vm.count("ant"))
        usrp->set_rx_antenna(ant, channel);

    std::this_thread::sleep_for(std::chrono::milliseconds(int64_t(1000 * setup_time)));

    // Check LO lock
    if (!vm.count("skip-lo")) {
        check_locked_sensor(usrp->get_rx_sensor_names(channel),
            "lo_locked",
            [usrp, channel](const std::string& sensor_name) {
                return usrp->get_rx_sensor(sensor_name, channel);
            },
            setup_time);
    }

    std::signal(SIGINT, &sig_int_handler);
    std::cout << "Press Ctrl + C to stop..." << std::endl;

    if (wirefmt == "s16") {
        throw std::runtime_error("This tool requires complex samples (sc16, sc8)");
    }

    // Pass empty file string if auto-filename mode
    std::string output_file = auto_filename ? "" : file;

    if (trig_enabled) {
        // Triggered recording mode
        if (type == "double")
            recv_to_file_triggered<std::complex<double>>(usrp, "fc64", wirefmt, channel, output_file, spb,
                total_time, threshold, hyst, detect_dur, pre_trig_time, null, continue_on_bad_packet,
                output_dir, hostname, freq, gain_val, file_desc, skip_first, start_retries, capture_count, dead_time);
        else if (type == "float")
            recv_to_file_triggered<std::complex<float>>(usrp, "fc32", wirefmt, channel, output_file, spb,
                total_time, threshold, hyst, detect_dur, pre_trig_time, null, continue_on_bad_packet,
                output_dir, hostname, freq, gain_val, file_desc, skip_first, start_retries, capture_count, dead_time);
        else if (type == "short")
            recv_to_file_triggered<std::complex<short>>(usrp, "sc16", wirefmt, channel, output_file, spb,
                total_time, threshold, hyst, detect_dur, pre_trig_time, null, continue_on_bad_packet,
                output_dir, hostname, freq, gain_val, file_desc, skip_first, start_retries, capture_count, dead_time);
        else
            throw std::runtime_error("Unknown type " + type);
    } else {
        // Immediate recording mode (no trigger)
        if (type == "double")
            recv_to_file_immediate<std::complex<double>>(usrp, "fc64", wirefmt, channel, output_file, spb,
                total_time, null, continue_on_bad_packet,
                output_dir, hostname, freq, gain_val, file_desc, skip_first, start_retries, wait_mode);
        else if (type == "float")
            recv_to_file_immediate<std::complex<float>>(usrp, "fc32", wirefmt, channel, output_file, spb,
                total_time, null, continue_on_bad_packet,
                output_dir, hostname, freq, gain_val, file_desc, skip_first, start_retries, wait_mode);
        else if (type == "short")
            recv_to_file_immediate<std::complex<short>>(usrp, "sc16", wirefmt, channel, output_file, spb,
                total_time, null, continue_on_bad_packet,
                output_dir, hostname, freq, gain_val, file_desc, skip_first, start_retries, wait_mode);
        else
            throw std::runtime_error("Unknown type " + type);
    }

    std::cout << std::endl << "Done!" << std::endl << std::endl;
    return EXIT_SUCCESS;
}
