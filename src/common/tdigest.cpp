////////////////////////////////////////////////////////////////////////////////
// tdigest
//
// Copyright (c) 2018 Andrew Werner, All rights reserved.
//
// tdigest is an implementation of Ted Dunning's streaming quantile estimation
// data structure. 
// This implementation is intended to be like the new MergingHistogram.
// It focuses on being in portable C that should be easy to integrate into other
// languages. In particular it provides mechanisms to preallocate all memory 
// at construction time.
//
// The implementation is a direct descendent of 
//  https://github.com/tdunning/t-digest/
//
// TODO: add a Ted Dunning Copyright notice.
//
////////////////////////////////////////////////////////////////////////////////

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <math.h>
#include "common.h"
#include "tdigest.h"

namespace baikaldb {
namespace tdigest {

static const double TD_PI = 3.14159265358979323846;
static const double EPSILON = (1e-9);

typedef struct node {
    double mean;
    double count;
} node_t;
struct td_histogram {
    char magic[4];      // "TDGT"
    // cap is the total size of nodes
    int cap;
    // compression is a setting used to configure the size of centroids when merged.
    double compression;
    // merged_nodes is the number of merged nodes at the front of nodes.
    int merged_nodes;
    // unmerged_nodes is the number of buffered nodes.
    int unmerged_nodes;
    double merged_count;
    double unmerged_count;
    node_t nodes[0];
};
static bool is_very_small(double val) {
    return !(val > 0.000000001 || val < -0.000000001);
}
static int cap_from_compression(double compression) {
    return (6 * (int)(compression)) + 10;
}
static bool should_merge(td_histogram_t *h) {
    return ((h->merged_nodes + h->unmerged_nodes) == h->cap);
}
static int next_node(td_histogram_t *h) {
    return h->merged_nodes + h->unmerged_nodes;
}
static void merge(td_histogram_t *h);
////////////////////////////////////////////////////////////////////////////////
// Constructors
////////////////////////////////////////////////////////////////////////////////
size_t td_required_buf_size(double compression) {
    return sizeof(td_histogram_t) + (cap_from_compression(compression) * sizeof(node_t));
}
size_t td_actual_size(td_histogram_t *h) {
    return sizeof(td_histogram_t) + ((h->merged_nodes + h->unmerged_nodes) * sizeof(node_t));
}
// td_init will initialize a td_histogram_t inside buf which is buf_size bytes.
// If buf_size is too small (smaller than compression + 1) or buf is NULL,
// the returned pointer will be NULL.
//
// In general use td_required_buf_size to figure out what size buffer to
// pass.
td_histogram_t* td_init(double compression, uint8_t *buf, size_t buf_size) {
    td_histogram_t *h = (td_histogram_t *)(buf);
    if (!h) {
        return NULL;
    }
    bzero((void *)(h), buf_size);
    h->magic[0] = 'T';
    h->magic[1] = 'D';
    h->magic[2] = 'G';
    h->magic[3] = 'T';
    h->compression = compression,
    h->cap = (buf_size - sizeof(td_histogram_t)) / sizeof(node_t);
    h->merged_nodes = 0;
    h->merged_count = 0;
    h->unmerged_nodes = 0;
    h->unmerged_count = 0;
    return h;
}

void td_merge(td_histogram_t *into, td_histogram_t *from) {
    merge(into);
    merge(from);
    for (int i = 0; i < from->merged_nodes; i++) {
        node_t *n = &from->nodes[i];
        td_add(into, n->mean, n->count);
    }
}
void td_reset(td_histogram_t *h) {
    bzero((void *)(&h->nodes[0]), sizeof(node_t) * h->cap);
    h->merged_nodes = 0;
    h->merged_count = 0;
    h->unmerged_nodes = 0;
    h->unmerged_count = 0;
}
void td_decay(td_histogram_t *h, double factor) {
    merge(h);
    h->unmerged_count *= factor;
    h->merged_count *= factor;
    for (int i = 0; i < h->merged_nodes; i++) {
        h->nodes[i].count *= factor;
    }
}
double td_total_count(td_histogram_t *h) {
    return h->merged_count + h->unmerged_count;
}
double td_total_sum(td_histogram_t *h) {
    node_t *n = NULL;
    double sum = 0;
    int total_nodes = h->merged_nodes + h->unmerged_nodes;
    for (int i = 0; i < total_nodes; i++) {
        n = &h->nodes[i];
        sum += n->mean * n->count;
    }
    return sum;
}
double td_quantile_of(td_histogram_t *h, double val) {
    merge(h);
    if (h->merged_nodes == 0) {
        return NAN;
    }
    double k = 0;
    int i = 0;
    node_t *n = NULL;
    for (i = 0; i < h->merged_nodes; i++) {
        n = &h->nodes[i];
        if (n->mean >= val) {
            break;
        }
        k += n->count;
    }
    if (fabs(val - n->mean) < EPSILON) {
        // technically this needs to find all of the nodes which contain this value and sum their
        // weight
        double count_at_value = n->count;
        for (i += 1; i < h->merged_nodes && h->nodes[i].mean == n->mean; i++) {
            count_at_value += h->nodes[i].count;
        }
        return (k + (count_at_value / 2)) / h->merged_count;
    } else if (val > n->mean) {  // past the largest
        return 1;
    } else if (i == 0) {
        return 0;
    }
    // we want to figure out where along the line from the prev node to this node, the value falls
    node_t *nr = n;
    node_t *nl = n - 1;
    k -= (nl->count / 2);
    // we say that at zero we're at nl->mean
    // and at (nl->count/2 + nr->count/2) we're at nr
    double m = (nr->mean - nl->mean) / (nl->count / 2 + nr->count / 2);
    double x = (val - nl->mean) / m;
    return (k + x) / h->merged_count;
}
double td_value_at(td_histogram_t *h, double q) {
    merge(h);
    if (q < 0 || q > 1 || h->merged_nodes == 0) {
        return NAN;
    }
    // if left of the first node, use the first node
    // if right of the last node, use the last node, use it
    double goal = q * h->merged_count;
    double k = 0;
    int i = 0;
    node_t *n = NULL;
    for (i = 0; i < h->merged_nodes; i++) {
        n = &h->nodes[i];
        if (k + n->count > goal) {
            break;
        }
        k += n->count;
    }
    double delta_k = goal - k - (n->count / 2);
    if (is_very_small(delta_k)) {
        return n->mean;
    }
    bool right = delta_k > 0;
    if ((right && ((i + 1) == h->merged_nodes)) || (!right && (i == 0))) {
        return n->mean;
    }
    node_t *nl;
    node_t *nr;
    if (right) {
        nl = n;
        nr = &h->nodes[i + 1];
        k += (nl->count / 2);
    } else {
        nl = &h->nodes[i - 1];
        nr = n;
        k -= (nl->count / 2);
    }
    double x = goal - k;
    // we have two points (0, nl->mean), (nr->count, nr->mean)
    // and we want x
    double m = (nr->mean - nl->mean) / (nl->count / 2 + nr->count / 2);
    return m * x + nl->mean;
}
double td_trimmed_mean(td_histogram_t *h, double lo, double hi) {
    merge(h);
    double total_count = h->merged_count;
    double left_tail_count = lo * total_count;
    double right_tail_count = hi * total_count;
    double count_seen = 0;
    double weighted_mean = 0;
    for (int i = 0; i < h->merged_nodes; i++) {
        if (i > 0) {
            count_seen += h->nodes[i - 1].count;
        }
        node_t *n = &h->nodes[i];
        if (n->count < left_tail_count) {
            continue;
        }
        if (count_seen > right_tail_count) {
            break;
        }
        double left = count_seen;
        if (left < left_tail_count) {
            left = left_tail_count;
        }
        double right = count_seen + n->count;
        if (right > right_tail_count) {
            right = right_tail_count;
        }
        weighted_mean += n->mean * (right - left);
    }
    double included_count = total_count * (hi - lo);
    return weighted_mean / included_count;
}
void td_add(td_histogram_t *h, double mean, double count) {
    if (should_merge(h)) {
        merge(h);
    }
    h->nodes[next_node(h)] = (node_t){
            .mean = mean,
            .count = count,
    };
    h->unmerged_nodes++;
    h->unmerged_count += count;
}
static int compare_nodes(const void *v1, const void *v2) {
    node_t *n1 = (node_t *)(v1);
    node_t *n2 = (node_t *)(v2);
    if (n1->mean < n2->mean) {
        return -1;
    } else if (n1->mean > n2->mean) {
        return 1;
    } else {
        return 0;
    }
}
static void merge(td_histogram_t *h) {
    if (h->unmerged_nodes == 0) {
        return;
    }
    int N = h->merged_nodes + h->unmerged_nodes;
    qsort((void *)(h->nodes), N, sizeof(node_t), &compare_nodes);
    double total_count = h->merged_count + h->unmerged_count;
    double denom = 2 * TD_PI * total_count * log(total_count);
    double normalizer = h->compression / denom;
    int cur = 0;
    double count_so_far = 0;
    for (int i = 1; i < N; i++) {
        double proposed_count = h->nodes[cur].count + h->nodes[i].count;
        double z = proposed_count * normalizer;
        double q0 = count_so_far / total_count;
        double q2 = (count_so_far + proposed_count) / total_count;
        bool should_add = (z <= (q0 * (1 - q0))) && (z <= (q2 * (1 - q2)));
        if (should_add) {
            h->nodes[cur].count += h->nodes[i].count;
            double delta = h->nodes[i].mean - h->nodes[cur].mean;
            double weighted_delta = (delta * h->nodes[i].count) / h->nodes[cur].count;
            h->nodes[cur].mean += weighted_delta;
        } else {
            count_so_far += h->nodes[cur].count;
            cur++;
            h->nodes[cur] = h->nodes[i];
        }
        if (cur != i) {
            h->nodes[i] = (node_t){
                    .mean = 0,
                    .count = 0,
            };
        }
    }
    h->merged_nodes = cur + 1;
    h->merged_count = total_count;
    h->unmerged_nodes = 0;
    h->unmerged_count = 0;
}

bool is_td_object(const std::string& td) {
    if (td.size() < 4) {
        DB_WARNING("size error");
        return false;
    }
    td_histogram_t* t = (td_histogram_t*)td.data();
    if (t->magic[0] != 'T' || t->magic[1] != 'D' ||
        t->magic[2] != 'G' || t->magic[3] != 'T') {
        DB_WARNING("magic error");
        return false;
    }
    if (td.size() < td_actual_size(t)) {
        DB_WARNING("size error size:%lu, td_actual_size:%lu", td.size(), td_actual_size(t));
        return false;
    }
    return true;
}

void td_serialize(std::string& td) {
    if (!is_td_object(td)) {
        return;
    }
    td_histogram_t* t = (td_histogram_t*)td.data();
    merge(t);
    td.resize(td_actual_size(t));
}

void td_normallize(std::string& td) {
    if (!is_td_object(td)) {
        return;
    }
    td_histogram_t* t = (td_histogram_t*)td.data();
    td.resize(td_required_buf_size(t->compression));
}

} // namespace tdigest
} // namespace baikaldb
