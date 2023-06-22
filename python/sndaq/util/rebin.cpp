#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <iostream>
#include <cmath>

namespace py = pybind11;


// Time-binnings of scalers (1.6384 ms) and rebinned-hits (2 ms) expressed in units 0.1 ns
unsigned long scaler_udt = 250 * std::pow(2, 16);
unsigned long raw_udt = 20000000;


/** Rebin a sequence of SN scalers from 1.6384 ms to 2 ms bins.
*
* Returns the scaler hits in 2 ms bins and the corresponding indices
*   assuming a destination array binned at 2 ms
*
* @param raw_utime
*       Time at which the destination 2 ms array begins
*       (Measured as UTC time since year start in 0.1 ns)
* @param payload_utime
*       Time at sequence of scalers begins, this will be the `payload.utime` field
*       (Measured as UTC time since year start in 0.1 ns)
* @param scaler_bytes
*       Sequence of SN scalers, assumed to be binned at 1.6384 ms.
*/
std::pair<py::array_t<unsigned int>, py::array_t<unsigned int>> rebin_scalers(
    unsigned long long raw_utime,
    const unsigned long long & payload_utime,
    const py::bytes & scaler_bytes) {

    std::vector<unsigned int> raw_counts;
    std::vector<unsigned int> idx_counts;

    unsigned long long scaler_utime;
    unsigned int raw_count = 0;
    double frac;
    double frac_count=0;
    unsigned int idx_raw = 0;
    std::string scalers = scaler_bytes;

    for (size_t idx_scaler=0; idx_scaler < scalers.length(); idx_scaler++) {
        unsigned long scaler = scalers[idx_scaler];

        // Only process non-zero scalers, if frac_count > 0, then the previous scaler overflowed into the current bin
        if (scaler > 0 || frac_count > 0) {
            scaler_utime = payload_utime + idx_scaler * scaler_udt;

            // Find 2 ms bin that corresponds to current scaler time
            while (raw_utime + raw_udt < scaler_utime ) {
                idx_raw++;
                raw_utime += raw_udt;
            }

            // Add full 1.6 ms bin to 2 ms bin
            raw_count += scaler;
            raw_count += frac_count;
            frac_count = 0;

            // Subtract fractional 1.6 ms bin from 2 ms bin
            if ((scaler_utime + scaler_udt > raw_utime + raw_udt) && (scaler_utime < raw_utime + raw_udt)){
                frac = 1. - ((double)(raw_utime + raw_udt - scaler_utime)/(double)scaler_udt);
                frac_count = (unsigned int)(0.5 + frac * (double)scaler);
                raw_count -= frac_count;
            }
            // Only record rebinned scalers if they are non-zero
            if (raw_count > 0) {
                raw_counts.push_back(raw_count);
                idx_counts.push_back(idx_raw);
                raw_count = 0;
                }
            }
    }
    // Add the last 1.6 ms scaler into another 2 ms bin if appropriate
    if (frac_count > 0) {
        raw_counts.push_back(raw_count);
        idx_counts.push_back(idx_raw+1);
       }

    return std::make_pair(py::cast(raw_counts), py::cast(idx_counts));
}

PYBIND11_MODULE(rebin, m) {
    m.doc() = "PyBind11 attempt at Rebinning";
    m.def("rebin_scalers", &rebin_scalers, "Test Function");
}
