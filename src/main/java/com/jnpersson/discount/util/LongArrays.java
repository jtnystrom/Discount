/*
 * Copyright (C) 2002-2021 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *
 * For the sorting and binary search code:
 *
 * Copyright (C) 1999 CERN - European Organization for Nuclear Research.
 *
 *   Permission to use, copy, modify, distribute and sell this software and
 *   its documentation for any purpose is hereby granted without fee,
 *   provided that the above copyright notice appear in all copies and that
 *   both that copyright notice and this permission notice appear in
 *   supporting documentation. CERN makes no representations about the
 *   suitability of this software for any purpose. It is provided "as is"
 *   without expressed or implied warranty.
 */

package com.jnpersson.discount.util;

/**
 * Adapted from Fastutil for Discount by Johan Nyström-Persson.
 */
public class LongArrays {
    private static final int DIGIT_BITS = 8;
    /** The mask to extract a digit of {@link #DIGIT_BITS} bits. */
    private static final int DIGIT_MASK = (1 << DIGIT_BITS) - 1;
    /** The number of digits per element. */
    private static final int DIGITS_PER_ELEMENT = Long.SIZE / DIGIT_BITS;

    //Key constant for tuning - currently 64 in Fastutil.
    //Below this constant we fall back to quadratic selection sort.
    private static final int RADIXSORT_NO_REC = 16;

    private static void selectionSort(final long[][] a, final int from, final int to, final int level) {
        final int layers = a.length;
        final int firstLayer = level / DIGITS_PER_ELEMENT;
        for(int i = from; i < to - 1; i++) {
            int m = i;
            for(int j = i + 1; j < to; j++) {
                for(int p = firstLayer; p < layers; p++) {
                    if (a[p][j] < a[p][m]) {
                        m = j;
                        break;
                    }
                    else if (a[p][j] > a[p][m]) break;
                }
            }
            if (m != i) {
                for(int p = layers; p-- != 0;) {
                    final long u = a[p][i];
                    a[p][i] = a[p][m];
                    a[p][m] = u;
                }
            }
        }
    }
    /** Sorts the specified array of arrays lexicographically using radix sort.
     *
     * <p>The sorting algorithm is a tuned radix sort adapted from Peter M. McIlroy, Keith Bostic and M. Douglas
     * McIlroy, &ldquo;Engineering radix sort&rdquo;, <i>Computing Systems</i>, 6(1), pages 5&minus;27 (1993).
     *
     * <p>This method implements a <em>lexicographical</em> sorting of the provided arrays. Tuples of elements
     * in the same position will be considered a single key, and permuted
     * accordingly.
     *
     * @param a an array containing arrays of equal length to be sorted lexicographically in parallel.
     */
    public static void radixSort(final long[][] a) {
        radixSort(a, 0, a[0].length);
    }
    /** Sorts the specified array of arrays lexicographically using radix sort.
     *
     * <p>The sorting algorithm is a tuned radix sort adapted from Peter M. McIlroy, Keith Bostic and M. Douglas
     * McIlroy, &ldquo;Engineering radix sort&rdquo;, <i>Computing Systems</i>, 6(1), pages 5&minus;27 (1993).
     *
     * <p>This method implements a <em>lexicographical</em> sorting of the provided arrays. Tuples of elements
     * in the same position will be considered a single key, and permuted
     * accordingly.
     *
     * @param a an array containing arrays of equal length to be sorted lexicographically in parallel.
     * @param from the index of the first element (inclusive) to be sorted.
     * @param to the index of the last element (exclusive) to be sorted.
     */
    public static void radixSort(final long[][] a, final int from, final int to) {
        if (to - from < RADIXSORT_NO_REC) {
            selectionSort(a, from, to, 0);
            return;
        }
        final int layers = a.length;
        final int maxLevel = DIGITS_PER_ELEMENT * layers - 1;
        for(int p = layers, l = a[0].length; p-- != 0;) if (a[p].length != l) throw new IllegalArgumentException("The array of index " + p + " has not the same length of the array of index 0.");
        final int stackSize = ((1 << DIGIT_BITS) - 1) * (layers * DIGITS_PER_ELEMENT - 1) + 1;
        int stackPos = 0;
        final int[] offsetStack = new int[stackSize];
        final int[] lengthStack = new int[stackSize];
        final int[] levelStack = new int[stackSize];
        offsetStack[stackPos] = from;
        lengthStack[stackPos] = to - from;
        levelStack[stackPos++] = 0;
        final int[] count = new int[1 << DIGIT_BITS];
        final int[] pos = new int[1 << DIGIT_BITS];
        final long[] t = new long[layers];
        while(stackPos > 0) {
            final int first = offsetStack[--stackPos];
            final int length = lengthStack[stackPos];
            final int level = levelStack[stackPos];
            final int signMask = level % DIGITS_PER_ELEMENT == 0 ? 1 << DIGIT_BITS - 1 : 0;
            final long[] k = a[level / DIGITS_PER_ELEMENT]; // This is the key array
            final int shift = (DIGITS_PER_ELEMENT - 1 - level % DIGITS_PER_ELEMENT) * DIGIT_BITS; // This is the shift that extract the right byte from a key
            // Count keys.
            for(int i = first + length; i-- != first;) count[(int)((k[i]) >>> shift & DIGIT_MASK ^ signMask)]++;
            // Compute cumulative distribution
            int lastUsed = -1;
            for (int i = 0, p = first; i < 1 << DIGIT_BITS; i++) {
                if (count[i] != 0) lastUsed = i;
                pos[i] = (p += count[i]);
            }
            final int end = first + length - count[lastUsed];
            // i moves through the start of each block
            for(int i = first, c = -1, d; i <= end; i += count[c], count[c] = 0) {
                for(int p = layers; p-- != 0;) t[p] = a[p][i];
                c = (int)((k[i]) >>> shift & DIGIT_MASK ^ signMask);
                if (i < end) { // When all slots are OK, the last slot is necessarily OK.
                    while((d = --pos[c]) > i) {
                        c = (int)((k[d]) >>> shift & DIGIT_MASK ^ signMask);
                        for(int p = layers; p-- != 0;) {
                            final long u = t[p];
                            t[p] = a[p][d];
                            a[p][d] = u;
                        }
                    }
                    for(int p = layers; p-- != 0;) a[p][i] = t[p];
                }
                if (level < maxLevel && count[c] > 1) {
                    if (count[c] < RADIXSORT_NO_REC) selectionSort(a, i, i + count[c], level + 1);
                    else {
                        offsetStack[stackPos] = i;
                        lengthStack[stackPos] = count[c];
                        levelStack[stackPos++] = level + 1;
                    }
                }
            }
        }
    }
}
