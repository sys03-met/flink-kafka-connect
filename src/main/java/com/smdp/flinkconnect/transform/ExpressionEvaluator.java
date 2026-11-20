package com.smdp.flinkconnect.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 컬럼 수식 평가기.
 *
 * 용도: YAML {@code op: expression} 문자열을 같은 메시지 row 위에서 계산한다.
 *
 * 지원: 필드명, 숫자 리터럴, {@code + - * /}, 괄호, 따옴표 문자열.
 * 제곱은 {@code A * A}. {@code ^}/pow/함수 호출 없음.
 *
 * 왜 외부 엔진을 안 쓰나: 의존성·보안 표면을 줄이고, YAML 수식 범위를 명시적으로 제한한다.
 *
 * 문법: 항(term)=곱나눗셈, 식(expression)=가감. 재귀 하강 파서.
 */
public final class ExpressionEvaluator {

  /** 토큰화 후 파싱. 빈 식은 null. */
  public Object evaluate(String expression, Map<String, Object> row) {
    if (expression == null || expression.isBlank()) {
      return null;
    }
    List<String> tokens = tokenize(expression.trim());
    return parseExpression(tokens, row);
  }

  /** 가감 (+ -). */
  private Object parseExpression(List<String> tokens, Map<String, Object> row) {
    Object left = parseTerm(tokens, row);
    while (!tokens.isEmpty() && (peek(tokens).equals("+") || peek(tokens).equals("-"))) {
      String op = tokens.removeFirst();
      Object right = parseTerm(tokens, row);
      left = applyAddSub(left, right, op);
    }
    return left;
  }

  /** 곱나눗셈 (* /). */
  private Object parseTerm(List<String> tokens, Map<String, Object> row) {
    Object left = parseFactor(tokens, row);
    while (!tokens.isEmpty() && (peek(tokens).equals("*") || peek(tokens).equals("/"))) {
      String op = tokens.removeFirst();
      Object right = parseFactor(tokens, row);
      left = applyMulDiv(left, right, op);
    }
    return left;
  }

  /** 괄호 · 단항 마이너스 · atom. */
  private Object parseFactor(List<String> tokens, Map<String, Object> row) {
    if (tokens.isEmpty()) {
      throw new IllegalArgumentException("Unexpected end of expression");
    }
    String t = tokens.removeFirst();
    if ("(".equals(t)) {
      Object v = parseExpression(tokens, row);
      if (tokens.isEmpty() || !")".equals(tokens.removeFirst())) {
        throw new IllegalArgumentException("Missing ')'");
      }
      return v;
    }
    if ("-".equals(t)) {
      Object v = parseFactor(tokens, row);
      return negate(v);
    }
    return atom(t, row);
  }

  /** 숫자 리터럴 · row 필드 · 따옴표 문자열. */
  private Object atom(String token, Map<String, Object> row) {
    if (isNumber(token)) {
      if (token.contains(".") || token.contains("e") || token.contains("E")) {
        return Double.valueOf(token);
      }
      return Long.valueOf(token);
    }
    if (row.containsKey(token)) {
      return row.get(token);
    }
    if ((token.startsWith("'") && token.endsWith("'"))
        || (token.startsWith("\"") && token.endsWith("\""))) {
      return token.substring(1, token.length() - 1);
    }
    throw new IllegalArgumentException("Unknown identifier: " + token);
  }

  /** + 는 숫자면 합, 비숫자면 문자열 연결. - 는 숫자만. */
  private static Object applyAddSub(Object left, Object right, String op) {
    if (left instanceof Number l && right instanceof Number r) {
      double lv = l.doubleValue();
      double rv = r.doubleValue();
      return "+".equals(op) ? lv + rv : lv - rv;
    }
    if ("+".equals(op)) {
      return String.valueOf(left) + String.valueOf(right);
    }
    throw new IllegalArgumentException("Cannot apply " + op + " to non-numbers");
  }

  private static Object applyMulDiv(Object left, Object right, String op) {
    if (!(left instanceof Number l) || !(right instanceof Number r)) {
      throw new IllegalArgumentException("Cannot apply " + op + " to non-numbers");
    }
    double lv = l.doubleValue();
    double rv = r.doubleValue();
    return "*".equals(op) ? lv * rv : lv / rv;
  }

  private static Object negate(Object v) {
    if (v instanceof Number n) {
      return -n.doubleValue();
    }
    throw new IllegalArgumentException("Cannot negate non-number");
  }

  private static String peek(List<String> tokens) {
    return tokens.getFirst();
  }

  private static boolean isNumber(String token) {
    try {
      Double.parseDouble(token);
      return true;
    } catch (NumberFormatException ex) {
      return false;
    }
  }

  /**
   * 식 → 토큰 목록.
   *
   * 식별자는 공백·연산자까지. 필드명에 연산자가 들어가면 안 된다.
   */
  static List<String> tokenize(String expr) {
    List<String> tokens = new ArrayList<>();
    int i = 0;
    while (i < expr.length()) {
      char c = expr.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if ("+-*/()".indexOf(c) >= 0) {
        tokens.add(String.valueOf(c));
        i++;
        continue;
      }
      if (c == '\'' || c == '"') {
        char quote = c;
        int j = i + 1;
        while (j < expr.length() && expr.charAt(j) != quote) {
          j++;
        }
        tokens.add(expr.substring(i, Math.min(j + 1, expr.length())));
        i = j + 1;
        continue;
      }
      int j = i;
      while (j < expr.length()) {
        char ch = expr.charAt(j);
        if (Character.isWhitespace(ch) || "+-*/()".indexOf(ch) >= 0) {
          break;
        }
        j++;
      }
      tokens.add(expr.substring(i, j));
      i = j;
    }
    return tokens;
  }
}
