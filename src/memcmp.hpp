#pragma once
#include <assert.h>
#include <immintrin.h>
#include <stdint.h>

extern "C" {

inline int memcmp128(const void *p1, const void *p2, size_t count) {
  const __m128i_u *s1 = (__m128i_u *)p1;
  const __m128i_u *s2 = (__m128i_u *)p2;

  while (count--) {
    __m128i item1 = _mm_lddqu_si128(s1++);
    __m128i item2 = _mm_lddqu_si128(s2++);
    __m128i result = _mm_cmpeq_epi64(item1, item2);
    if (!(unsigned int)_mm_test_all_ones(result)) {
      return -1;
    }
  }
  return 0;
}

inline int memcmp32(const void *str1, const void *str2, size_t count) {
  const uint32_t *s1 = (uint32_t *)str1;
  const uint32_t *s2 = (uint32_t *)str2;

  while (count--) {
    if (*s1++ != *s2++) {
      return -1;
    }
  }
  return 0;
}
}