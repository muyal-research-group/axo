from axo import Axo,axo_method
from typing import List
import numpy as np
import matplotlib.pyplot as plt

class HillClimb(Axo):
    
    def f(self,x:float)->float:
        return -x**2 + 5
    
    def neighborhood(self,x:float)->List[float]:
        step = 0.1
        return [x + step, x - step]
    
    @axo_method
    def hill_climb(self, x0:float, max_iters=1000, tolerance=1e-6,**kwargs):
        x = x0
        fx = self.f(x)
        
        for i in range(max_iters):
            improved = False
            for x_new in self.neighborhood(x):
                fx_new = self.f(x_new)
                if fx_new > fx + tolerance:
                    x = x_new
                    fx = fx_new
                    improved = True
                    break  # Solo tomamos la primera mejora

            if not improved:
                break  # Llegamos a un óptimo local

        return x, fx

    def plot(self,x_opt:float, fx_opt:float):
        x_vals = np.linspace(-3, 3, 200)
        y_vals = [self.f(x) for x in x_vals]

        # Graficamos
        plt.figure(figsize=(8, 6))
        plt.plot(x_vals, y_vals, label="f(x) = -x² + 5")
        plt.scatter([x_opt], [fx_opt], color="red", zorder=5, label=f"Óptimo local: x={x_opt:.2f}")
        plt.axhline(0, color="grey", linestyle="--", alpha=0.5)
        plt.title("Función objetivo y óptimo hallado")
        plt.xlabel("x")
        plt.ylabel("f(x)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()
        return 0
            


class Object1(Axo):
    @axo_method
    def run(self,x:int):
        return 42  # constant-time operation


class Object2(Axo):
    @axo_method
    def run(self, payload):
        data = sorted(payload["numbers"])
        target = payload["target"]
        left, right = 0, len(data) - 1
        while left <= right:
            mid = (left + right) // 2
            if data[mid] == target:
                return mid
            elif data[mid] < target:
                left = mid + 1
            else:
                right = mid - 1
        return -1

    
class Object3(Axo):
    @axo_method
    def run(self,n):  # exponential time for small n (but we simulate it as O(n²) for load)
        if n <= 1:
            return n
        prev, curr = 0, 1
        for _ in range(2, n + 1):
            prev, curr = curr, prev + curr
        return curr

class Object4(Axo):
    @axo_method    
    def run(self, payload):
        def merge_sort(arr):
            if len(arr) <= 1:
                return arr
            mid = len(arr) // 2
            left = merge_sort(arr[:mid])
            right = merge_sort(arr[mid:])
            return merge(left, right)

        def merge(left, right):
            result = []
            while left and right:
                if left[0] < right[0]:
                    result.append(left.pop(0))
                else:
                    result.append(right.pop(0))
            return result + left + right

        return merge_sort(payload["numbers"] )


class Object5(Axo):
    @axo_method
    def run(self, payload):
        data = payload["points"]
        n = len(data)
        distances = []
        for i in range(n):
            for j in range(n):
                if i != j:
                    d = abs(data[i] - data[j])
                    distances.append(d)
        return distances

class Object6(Axo):
    @axo_method
    def run(self, payload):
        A = payload["A"]
        B = payload["B"]
        n = len(A)
        result = [[0]*n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                for k in range(n):
                    result[i][j] += A[i][k] * B[k][j]
        return result

class Object7(Axo):
    @axo_method
    def run(self, numbers):  # O(n^2)
        count = 0
        n = len(numbers)
        for i in range(n):
            for j in range(n):
                if i != j and numbers[i] + numbers[j] == 10:
                    count += 1
        return count

class Object8(Axo):
    @axo_method   
    def run(self, payload):
        from itertools import permutations
        cities = payload["cities"]
        all_routes = list(permutations(cities))
        return len(all_routes)  # Or evaluate cost of each route

class LowAO(Axo):
    def run(self, x):
        # Simple computation: adds one to the input
        # This is a constant time operation
        return x + 1

class Low1AO(Axo):
    def run(self):
        return 42


class Low2AO(Axo):
    def run(self, x):
        return x * 2


class Low3AO(Axo):
    def run(self, text):
        return text[0]


class Low4AO(Axo):
    def run(self):
        return "Hello, world!"


class Low5AO(Axo):
    def run(self, x):
        return x + 1


class Low6AO(Axo):
    def run(self, x):
        return -x


class Low7AO(Axo):
    def run(self, x):
        return x * x


class Low8AO(Axo):
    def run(self):
        return True


class Low9AO(Axo):
    def run(self, x):
        return x % 10


class Low10AO(Axo):
    def run(self, text):
        return text.upper()



# medium_class_aos.py

class Medium1AO(Axo):
    def run(self, xs):
        total = 0
        for x in xs:
            if isinstance(x, int):
                total += x
        if total > 100:
            total = 100
        return total


class Medium2AO(Axo):
    def run(self, xs):
        even = []
        for x in xs:
            if x % 2 == 0:
                even.append(x)
        result = []
        for e in even:
            result.append(e * 2)
        return result


class Medium3AO(Axo):
    def run(self, text):
        counts = {}
        for c in text:
            if c in counts:
                counts[c] += 1
            else:
                counts[c] = 1
        result = []
        for k in counts:
            if counts[k] > 1:
                result.append((k, counts[k]))
        return result


class Medium4AO(Axo):
    def run(self, xs):
        filtered = []
        for x in xs:
            if x > 0 and x < 100:
                filtered.append(x)
        average = 0
        if filtered:
            total = 0
            for x in filtered:
                total += x
            average = total / len(filtered)
        return average


class Medium5AO(Axo):
    def run(self, text):
        words = text.split()
        long_words = []
        for w in words:
            if len(w) > 5:
                long_words.append(w.upper())
        return long_words


class Medium6AO(Axo):
    def run(self, xs):
        sorted_list = []
        for x in xs:
            inserted = False
            for i in range(len(sorted_list)):
                if x < sorted_list[i]:
                    sorted_list.insert(i, x)
                    inserted = True
                    break
            if not inserted:
                sorted_list.append(x)
        return sorted_list


class Medium7AO(Axo):
    def run(self, text):
        vowels = "aeiouAEIOU"
        count = 0
        for c in text:
            if c in vowels:
                count += 1
        return {"vowel_count": count, "length": len(text)}


class Medium8AO(Axo):
    def run(self, xs):
        result = []
        for i in range(len(xs)):
            value = xs[i]
            if i % 2 == 0:
                result.append(value * 2)
            else:
                result.append(value + 1)
        return result


class Medium9AO(Axo):
    def run(self, xs):
        seen = set()
        unique = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                unique.append(x)
        return unique


class Medium10AO(Axo):
    def run(self, text):
        cleaned = []
        for c in text:
            if c.isalpha():
                cleaned.append(c.lower())
        reversed_text = ""
        for i in range(len(cleaned) - 1, -1, -1):
            reversed_text += cleaned[i]
        return reversed_text



# high_class_aos.py

class High1AO(Axo):
    def run(self, xs):
        stats = {
            "count": 0,
            "sum": 0,
            "max": None,
            "min": None,
            "evens": 0,
            "odds": 0,
        }

        for x in xs:
            stats["count"] += 1
            stats["sum"] += x
            if stats["max"] is None or x > stats["max"]:
                stats["max"] = x
            if stats["min"] is None or x < stats["min"]:
                stats["min"] = x
            if x % 2 == 0:
                stats["evens"] += 1
            else:
                stats["odds"] += 1

        counts = {}
        for x in xs:
            if x not in counts:
                counts[x] = 0
            counts[x] += 1

        top_values = []
        for key in counts:
            if counts[key] > 1:
                top_values.append((key, counts[key]))

        summary = {}
        summary["statistics"] = stats
        summary["duplicates"] = top_values

        # Relleno artificial
        padding = 0
        for _ in range(100):
            for _ in range(5):
                padding += 1
                padding -= 1
                padding += 2
                padding -= 2
                padding += 3
                padding -= 3
                padding += 4
                padding -= 4
                padding += 5
                padding -= 5

        summary["padding_check"] = padding
        return summary


class High2AO(Axo):
    def run(self, text):
        result = {
            "words": 0,
            "long_words": 0,
            "short_words": 0,
            "lines": 0,
            "letters": 0,
            "vowels": 0,
            "consonants": 0,
            "symbols": 0,
        }

        lines = text.split("\n")
        result["lines"] = len(lines)

        vowels = "aeiouAEIOU"
        for line in lines:
            words = line.split()
            result["words"] += len(words)
            for word in words:
                if len(word) > 6:
                    result["long_words"] += 1
                else:
                    result["short_words"] += 1
                for c in word:
                    if c.isalpha():
                        result["letters"] += 1
                        if c in vowels:
                            result["vowels"] += 1
                        else:
                            result["consonants"] += 1
                    else:
                        result["symbols"] += 1

        # Relleno artificial: procesamiento innecesario
        score = 0
        for i in range(50):
            for j in range(10):
                for k in range(2):
                    score += i * j + k
                    score -= j * k - i
                    score += k * k
                    score += j * j
                    score += i * i
                    score -= k + j + i
                    score += 1

        result["text_score"] = score

        # Más padding para aumentar las LOC
        dummy = ""
        for i in range(20):
            dummy += "line" + str(i) + ": "
            dummy += "x" * i
            dummy += "\n"
        result["dummy_output"] = dummy[:100]  # simula solo mostrar parte

        return result
class High1kAO(Axo):
    def run(self, x):
        result = []
        result.append('line_0: ' + str(0 * x))
        result.append('line_1: ' + str(1 * x))
        result.append('line_2: ' + str(2 * x))
        result.append('line_3: ' + str(3 * x))
        result.append('line_4: ' + str(4 * x))
        result.append('line_5: ' + str(5 * x))
        result.append('line_6: ' + str(6 * x))
        result.append('line_7: ' + str(7 * x))
        result.append('line_8: ' + str(8 * x))
        result.append('line_9: ' + str(9 * x))
        result.append('line_10: ' + str(10 * x))
        result.append('line_11: ' + str(11 * x))
        result.append('line_12: ' + str(12 * x))
        result.append('line_13: ' + str(13 * x))
        result.append('line_14: ' + str(14 * x))
        result.append('line_15: ' + str(15 * x))
        result.append('line_16: ' + str(16 * x))
        result.append('line_17: ' + str(17 * x))
        result.append('line_18: ' + str(18 * x))
        result.append('line_19: ' + str(19 * x))
        result.append('line_20: ' + str(20 * x))
        result.append('line_21: ' + str(21 * x))
        result.append('line_22: ' + str(22 * x))
        result.append('line_23: ' + str(23 * x))
        result.append('line_24: ' + str(24 * x))
        result.append('line_25: ' + str(25 * x))
        result.append('line_26: ' + str(26 * x))
        result.append('line_27: ' + str(27 * x))
        result.append('line_28: ' + str(28 * x))
        result.append('line_29: ' + str(29 * x))
        result.append('line_30: ' + str(30 * x))
        result.append('line_31: ' + str(31 * x))
        result.append('line_32: ' + str(32 * x))
        result.append('line_33: ' + str(33 * x))
        result.append('line_34: ' + str(34 * x))
        result.append('line_35: ' + str(35 * x))
        result.append('line_36: ' + str(36 * x))
        result.append('line_37: ' + str(37 * x))
        result.append('line_38: ' + str(38 * x))
        result.append('line_39: ' + str(39 * x))
        result.append('line_40: ' + str(40 * x))
        result.append('line_41: ' + str(41 * x))
        result.append('line_42: ' + str(42 * x))
        result.append('line_43: ' + str(43 * x))
        result.append('line_44: ' + str(44 * x))
        result.append('line_45: ' + str(45 * x))
        result.append('line_46: ' + str(46 * x))
        result.append('line_47: ' + str(47 * x))
        result.append('line_48: ' + str(48 * x))
        result.append('line_49: ' + str(49 * x))
        result.append('line_50: ' + str(50 * x))
        result.append('line_51: ' + str(51 * x))
        result.append('line_52: ' + str(52 * x))
        result.append('line_53: ' + str(53 * x))
        result.append('line_54: ' + str(54 * x))
        result.append('line_55: ' + str(55 * x))
        result.append('line_56: ' + str(56 * x))
        result.append('line_57: ' + str(57 * x))
        result.append('line_58: ' + str(58 * x))
        result.append('line_59: ' + str(59 * x))
        result.append('line_60: ' + str(60 * x))
        result.append('line_61: ' + str(61 * x))
        result.append('line_62: ' + str(62 * x))
        result.append('line_63: ' + str(63 * x))
        result.append('line_64: ' + str(64 * x))
        result.append('line_65: ' + str(65 * x))
        result.append('line_66: ' + str(66 * x))
        result.append('line_67: ' + str(67 * x))
        result.append('line_68: ' + str(68 * x))
        result.append('line_69: ' + str(69 * x))
        result.append('line_70: ' + str(70 * x))
        result.append('line_71: ' + str(71 * x))
        result.append('line_72: ' + str(72 * x))
        result.append('line_73: ' + str(73 * x))
        result.append('line_74: ' + str(74 * x))
        result.append('line_75: ' + str(75 * x))
        result.append('line_76: ' + str(76 * x))
        result.append('line_77: ' + str(77 * x))
        result.append('line_78: ' + str(78 * x))
        result.append('line_79: ' + str(79 * x))
        result.append('line_80: ' + str(80 * x))
        result.append('line_81: ' + str(81 * x))
        result.append('line_82: ' + str(82 * x))
        result.append('line_83: ' + str(83 * x))
        result.append('line_84: ' + str(84 * x))
        result.append('line_85: ' + str(85 * x))
        result.append('line_86: ' + str(86 * x))
        result.append('line_87: ' + str(87 * x))
        result.append('line_88: ' + str(88 * x))
        result.append('line_89: ' + str(89 * x))
        result.append('line_90: ' + str(90 * x))
        result.append('line_91: ' + str(91 * x))
        result.append('line_92: ' + str(92 * x))
        result.append('line_93: ' + str(93 * x))
        result.append('line_94: ' + str(94 * x))
        result.append('line_95: ' + str(95 * x))
        result.append('line_96: ' + str(96 * x))
        result.append('line_97: ' + str(97 * x))
        result.append('line_98: ' + str(98 * x))
        result.append('line_99: ' + str(99 * x))
        result.append('line_100: ' + str(100 * x))
        result.append('line_101: ' + str(101 * x))
        result.append('line_102: ' + str(102 * x))
        result.append('line_103: ' + str(103 * x))
        result.append('line_104: ' + str(104 * x))
        result.append('line_105: ' + str(105 * x))
        result.append('line_106: ' + str(106 * x))
        result.append('line_107: ' + str(107 * x))
        result.append('line_108: ' + str(108 * x))
        result.append('line_109: ' + str(109 * x))
        result.append('line_110: ' + str(110 * x))
        result.append('line_111: ' + str(111 * x))
        result.append('line_112: ' + str(112 * x))
        result.append('line_113: ' + str(113 * x))
        result.append('line_114: ' + str(114 * x))
        result.append('line_115: ' + str(115 * x))
        result.append('line_116: ' + str(116 * x))
        result.append('line_117: ' + str(117 * x))
        result.append('line_118: ' + str(118 * x))
        result.append('line_119: ' + str(119 * x))
        result.append('line_120: ' + str(120 * x))
        result.append('line_121: ' + str(121 * x))
        result.append('line_122: ' + str(122 * x))
        result.append('line_123: ' + str(123 * x))
        result.append('line_124: ' + str(124 * x))
        result.append('line_125: ' + str(125 * x))
        result.append('line_126: ' + str(126 * x))
        result.append('line_127: ' + str(127 * x))
        result.append('line_128: ' + str(128 * x))
        result.append('line_129: ' + str(129 * x))
        result.append('line_130: ' + str(130 * x))
        result.append('line_131: ' + str(131 * x))
        result.append('line_132: ' + str(132 * x))
        result.append('line_133: ' + str(133 * x))
        result.append('line_134: ' + str(134 * x))
        result.append('line_135: ' + str(135 * x))
        result.append('line_136: ' + str(136 * x))
        result.append('line_137: ' + str(137 * x))
        result.append('line_138: ' + str(138 * x))
        result.append('line_139: ' + str(139 * x))
        result.append('line_140: ' + str(140 * x))
        result.append('line_141: ' + str(141 * x))
        result.append('line_142: ' + str(142 * x))
        result.append('line_143: ' + str(143 * x))
        result.append('line_144: ' + str(144 * x))
        result.append('line_145: ' + str(145 * x))
        result.append('line_146: ' + str(146 * x))
        result.append('line_147: ' + str(147 * x))
        result.append('line_148: ' + str(148 * x))
        result.append('line_149: ' + str(149 * x))
        result.append('line_150: ' + str(150 * x))
        result.append('line_151: ' + str(151 * x))
        result.append('line_152: ' + str(152 * x))
        result.append('line_153: ' + str(153 * x))
        result.append('line_154: ' + str(154 * x))
        result.append('line_155: ' + str(155 * x))
        result.append('line_156: ' + str(156 * x))
        result.append('line_157: ' + str(157 * x))
        result.append('line_158: ' + str(158 * x))
        result.append('line_159: ' + str(159 * x))
        result.append('line_160: ' + str(160 * x))
        result.append('line_161: ' + str(161 * x))
        result.append('line_162: ' + str(162 * x))
        result.append('line_163: ' + str(163 * x))
        result.append('line_164: ' + str(164 * x))
        result.append('line_165: ' + str(165 * x))
        result.append('line_166: ' + str(166 * x))
        result.append('line_167: ' + str(167 * x))
        result.append('line_168: ' + str(168 * x))
        result.append('line_169: ' + str(169 * x))
        result.append('line_170: ' + str(170 * x))
        result.append('line_171: ' + str(171 * x))
        result.append('line_172: ' + str(172 * x))
        result.append('line_173: ' + str(173 * x))
        result.append('line_174: ' + str(174 * x))
        result.append('line_175: ' + str(175 * x))
        result.append('line_176: ' + str(176 * x))
        result.append('line_177: ' + str(177 * x))
        result.append('line_178: ' + str(178 * x))
        result.append('line_179: ' + str(179 * x))
        result.append('line_180: ' + str(180 * x))
        result.append('line_181: ' + str(181 * x))
        result.append('line_182: ' + str(182 * x))
        result.append('line_183: ' + str(183 * x))
        result.append('line_184: ' + str(184 * x))
        result.append('line_185: ' + str(185 * x))
        result.append('line_186: ' + str(186 * x))
        result.append('line_187: ' + str(187 * x))
        result.append('line_188: ' + str(188 * x))
        result.append('line_189: ' + str(189 * x))
        result.append('line_190: ' + str(190 * x))
        result.append('line_191: ' + str(191 * x))
        result.append('line_192: ' + str(192 * x))
        result.append('line_193: ' + str(193 * x))
        result.append('line_194: ' + str(194 * x))
        result.append('line_195: ' + str(195 * x))
        result.append('line_196: ' + str(196 * x))
        result.append('line_197: ' + str(197 * x))
        result.append('line_198: ' + str(198 * x))
        result.append('line_199: ' + str(199 * x))
        result.append('line_200: ' + str(200 * x))
        result.append('line_201: ' + str(201 * x))
        result.append('line_202: ' + str(202 * x))
        result.append('line_203: ' + str(203 * x))
        result.append('line_204: ' + str(204 * x))
        result.append('line_205: ' + str(205 * x))
        result.append('line_206: ' + str(206 * x))
        result.append('line_207: ' + str(207 * x))
        result.append('line_208: ' + str(208 * x))
        result.append('line_209: ' + str(209 * x))
        result.append('line_210: ' + str(210 * x))
        result.append('line_211: ' + str(211 * x))
        result.append('line_212: ' + str(212 * x))
        result.append('line_213: ' + str(213 * x))
        result.append('line_214: ' + str(214 * x))
        result.append('line_215: ' + str(215 * x))
        result.append('line_216: ' + str(216 * x))
        result.append('line_217: ' + str(217 * x))
        result.append('line_218: ' + str(218 * x))
        result.append('line_219: ' + str(219 * x))
        result.append('line_220: ' + str(220 * x))
        result.append('line_221: ' + str(221 * x))
        result.append('line_222: ' + str(222 * x))
        result.append('line_223: ' + str(223 * x))
        result.append('line_224: ' + str(224 * x))
        result.append('line_225: ' + str(225 * x))
        result.append('line_226: ' + str(226 * x))
        result.append('line_227: ' + str(227 * x))
        result.append('line_228: ' + str(228 * x))
        result.append('line_229: ' + str(229 * x))
        result.append('line_230: ' + str(230 * x))
        result.append('line_231: ' + str(231 * x))
        result.append('line_232: ' + str(232 * x))
        result.append('line_233: ' + str(233 * x))
        result.append('line_234: ' + str(234 * x))
        result.append('line_235: ' + str(235 * x))
        result.append('line_236: ' + str(236 * x))
        result.append('line_237: ' + str(237 * x))
        result.append('line_238: ' + str(238 * x))
        result.append('line_239: ' + str(239 * x))
        result.append('line_240: ' + str(240 * x))
        result.append('line_241: ' + str(241 * x))
        result.append('line_242: ' + str(242 * x))
        result.append('line_243: ' + str(243 * x))
        result.append('line_244: ' + str(244 * x))
        result.append('line_245: ' + str(245 * x))
        result.append('line_246: ' + str(246 * x))
        result.append('line_247: ' + str(247 * x))
        result.append('line_248: ' + str(248 * x))
        result.append('line_249: ' + str(249 * x))
        result.append('line_250: ' + str(250 * x))
        result.append('line_251: ' + str(251 * x))
        result.append('line_252: ' + str(252 * x))
        result.append('line_253: ' + str(253 * x))
        result.append('line_254: ' + str(254 * x))
        result.append('line_255: ' + str(255 * x))
        result.append('line_256: ' + str(256 * x))
        result.append('line_257: ' + str(257 * x))
        result.append('line_258: ' + str(258 * x))
        result.append('line_259: ' + str(259 * x))
        result.append('line_260: ' + str(260 * x))
        result.append('line_261: ' + str(261 * x))
        result.append('line_262: ' + str(262 * x))
        result.append('line_263: ' + str(263 * x))
        result.append('line_264: ' + str(264 * x))
        result.append('line_265: ' + str(265 * x))
        result.append('line_266: ' + str(266 * x))
        result.append('line_267: ' + str(267 * x))
        result.append('line_268: ' + str(268 * x))
        result.append('line_269: ' + str(269 * x))
        result.append('line_270: ' + str(270 * x))
        result.append('line_271: ' + str(271 * x))
        result.append('line_272: ' + str(272 * x))
        result.append('line_273: ' + str(273 * x))
        result.append('line_274: ' + str(274 * x))
        result.append('line_275: ' + str(275 * x))
        result.append('line_276: ' + str(276 * x))
        result.append('line_277: ' + str(277 * x))
        result.append('line_278: ' + str(278 * x))
        result.append('line_279: ' + str(279 * x))
        result.append('line_280: ' + str(280 * x))
        result.append('line_281: ' + str(281 * x))
        result.append('line_282: ' + str(282 * x))
        result.append('line_283: ' + str(283 * x))
        result.append('line_284: ' + str(284 * x))
        result.append('line_285: ' + str(285 * x))
        result.append('line_286: ' + str(286 * x))
        result.append('line_287: ' + str(287 * x))
        result.append('line_288: ' + str(288 * x))
        result.append('line_289: ' + str(289 * x))
        result.append('line_290: ' + str(290 * x))
        result.append('line_291: ' + str(291 * x))
        result.append('line_292: ' + str(292 * x))
        result.append('line_293: ' + str(293 * x))
        result.append('line_294: ' + str(294 * x))
        result.append('line_295: ' + str(295 * x))
        result.append('line_296: ' + str(296 * x))
        result.append('line_297: ' + str(297 * x))
        result.append('line_298: ' + str(298 * x))
        result.append('line_299: ' + str(299 * x))
        result.append('line_300: ' + str(300 * x))
        result.append('line_301: ' + str(301 * x))
        result.append('line_302: ' + str(302 * x))
        result.append('line_303: ' + str(303 * x))
        result.append('line_304: ' + str(304 * x))
        result.append('line_305: ' + str(305 * x))
        result.append('line_306: ' + str(306 * x))
        result.append('line_307: ' + str(307 * x))
        result.append('line_308: ' + str(308 * x))
        result.append('line_309: ' + str(309 * x))
        result.append('line_310: ' + str(310 * x))
        result.append('line_311: ' + str(311 * x))
        result.append('line_312: ' + str(312 * x))
        result.append('line_313: ' + str(313 * x))
        result.append('line_314: ' + str(314 * x))
        result.append('line_315: ' + str(315 * x))
        result.append('line_316: ' + str(316 * x))
        result.append('line_317: ' + str(317 * x))
        result.append('line_318: ' + str(318 * x))
        result.append('line_319: ' + str(319 * x))
        result.append('line_320: ' + str(320 * x))
        result.append('line_321: ' + str(321 * x))
        result.append('line_322: ' + str(322 * x))
        result.append('line_323: ' + str(323 * x))
        result.append('line_324: ' + str(324 * x))
        result.append('line_325: ' + str(325 * x))
        result.append('line_326: ' + str(326 * x))
        result.append('line_327: ' + str(327 * x))
        result.append('line_328: ' + str(328 * x))
        result.append('line_329: ' + str(329 * x))
        result.append('line_330: ' + str(330 * x))
        result.append('line_331: ' + str(331 * x))
        result.append('line_332: ' + str(332 * x))
        result.append('line_333: ' + str(333 * x))
        result.append('line_334: ' + str(334 * x))
        result.append('line_335: ' + str(335 * x))
        result.append('line_336: ' + str(336 * x))
        result.append('line_337: ' + str(337 * x))
        result.append('line_338: ' + str(338 * x))
        result.append('line_339: ' + str(339 * x))
        result.append('line_340: ' + str(340 * x))
        result.append('line_341: ' + str(341 * x))
        result.append('line_342: ' + str(342 * x))
        result.append('line_343: ' + str(343 * x))
        result.append('line_344: ' + str(344 * x))
        result.append('line_345: ' + str(345 * x))
        result.append('line_346: ' + str(346 * x))
        result.append('line_347: ' + str(347 * x))
        result.append('line_348: ' + str(348 * x))
        result.append('line_349: ' + str(349 * x))
        result.append('line_350: ' + str(350 * x))
        result.append('line_351: ' + str(351 * x))
        result.append('line_352: ' + str(352 * x))
        result.append('line_353: ' + str(353 * x))
        result.append('line_354: ' + str(354 * x))
        result.append('line_355: ' + str(355 * x))
        result.append('line_356: ' + str(356 * x))
        result.append('line_357: ' + str(357 * x))
        result.append('line_358: ' + str(358 * x))
        result.append('line_359: ' + str(359 * x))
        result.append('line_360: ' + str(360 * x))
        result.append('line_361: ' + str(361 * x))
        result.append('line_362: ' + str(362 * x))
        result.append('line_363: ' + str(363 * x))
        result.append('line_364: ' + str(364 * x))
        result.append('line_365: ' + str(365 * x))
        result.append('line_366: ' + str(366 * x))
        result.append('line_367: ' + str(367 * x))
        result.append('line_368: ' + str(368 * x))
        result.append('line_369: ' + str(369 * x))
        result.append('line_370: ' + str(370 * x))
        result.append('line_371: ' + str(371 * x))
        result.append('line_372: ' + str(372 * x))
        result.append('line_373: ' + str(373 * x))
        result.append('line_374: ' + str(374 * x))
        result.append('line_375: ' + str(375 * x))
        result.append('line_376: ' + str(376 * x))
        result.append('line_377: ' + str(377 * x))
        result.append('line_378: ' + str(378 * x))
        result.append('line_379: ' + str(379 * x))
        result.append('line_380: ' + str(380 * x))
        result.append('line_381: ' + str(381 * x))
        result.append('line_382: ' + str(382 * x))
        result.append('line_383: ' + str(383 * x))
        result.append('line_384: ' + str(384 * x))
        result.append('line_385: ' + str(385 * x))
        result.append('line_386: ' + str(386 * x))
        result.append('line_387: ' + str(387 * x))
        result.append('line_388: ' + str(388 * x))
        result.append('line_389: ' + str(389 * x))
        result.append('line_390: ' + str(390 * x))
        result.append('line_391: ' + str(391 * x))
        result.append('line_392: ' + str(392 * x))
        result.append('line_393: ' + str(393 * x))
        result.append('line_394: ' + str(394 * x))
        result.append('line_395: ' + str(395 * x))
        result.append('line_396: ' + str(396 * x))
        result.append('line_397: ' + str(397 * x))
        result.append('line_398: ' + str(398 * x))
        result.append('line_399: ' + str(399 * x))
        result.append('line_400: ' + str(400 * x))
        result.append('line_401: ' + str(401 * x))
        result.append('line_402: ' + str(402 * x))
        result.append('line_403: ' + str(403 * x))
        result.append('line_404: ' + str(404 * x))
        result.append('line_405: ' + str(405 * x))
        result.append('line_406: ' + str(406 * x))
        result.append('line_407: ' + str(407 * x))
        result.append('line_408: ' + str(408 * x))
        result.append('line_409: ' + str(409 * x))
        result.append('line_410: ' + str(410 * x))
        result.append('line_411: ' + str(411 * x))
        result.append('line_412: ' + str(412 * x))
        result.append('line_413: ' + str(413 * x))
        result.append('line_414: ' + str(414 * x))
        result.append('line_415: ' + str(415 * x))
        result.append('line_416: ' + str(416 * x))
        result.append('line_417: ' + str(417 * x))
        result.append('line_418: ' + str(418 * x))
        result.append('line_419: ' + str(419 * x))
        result.append('line_420: ' + str(420 * x))
        result.append('line_421: ' + str(421 * x))
        result.append('line_422: ' + str(422 * x))
        result.append('line_423: ' + str(423 * x))
        result.append('line_424: ' + str(424 * x))
        result.append('line_425: ' + str(425 * x))
        result.append('line_426: ' + str(426 * x))
        result.append('line_427: ' + str(427 * x))
        result.append('line_428: ' + str(428 * x))
        result.append('line_429: ' + str(429 * x))
        result.append('line_430: ' + str(430 * x))
        result.append('line_431: ' + str(431 * x))
        result.append('line_432: ' + str(432 * x))
        result.append('line_433: ' + str(433 * x))
        result.append('line_434: ' + str(434 * x))
        result.append('line_435: ' + str(435 * x))
        result.append('line_436: ' + str(436 * x))
        result.append('line_437: ' + str(437 * x))
        result.append('line_438: ' + str(438 * x))
        result.append('line_439: ' + str(439 * x))
        result.append('line_440: ' + str(440 * x))
        result.append('line_441: ' + str(441 * x))
        result.append('line_442: ' + str(442 * x))
        result.append('line_443: ' + str(443 * x))
        result.append('line_444: ' + str(444 * x))
        result.append('line_445: ' + str(445 * x))
        result.append('line_446: ' + str(446 * x))
        result.append('line_447: ' + str(447 * x))
        result.append('line_448: ' + str(448 * x))
        result.append('line_449: ' + str(449 * x))
        result.append('line_450: ' + str(450 * x))
        result.append('line_451: ' + str(451 * x))
        result.append('line_452: ' + str(452 * x))
        result.append('line_453: ' + str(453 * x))
        result.append('line_454: ' + str(454 * x))
        result.append('line_455: ' + str(455 * x))
        result.append('line_456: ' + str(456 * x))
        result.append('line_457: ' + str(457 * x))
        result.append('line_458: ' + str(458 * x))
        result.append('line_459: ' + str(459 * x))
        result.append('line_460: ' + str(460 * x))
        result.append('line_461: ' + str(461 * x))
        result.append('line_462: ' + str(462 * x))
        result.append('line_463: ' + str(463 * x))
        result.append('line_464: ' + str(464 * x))
        result.append('line_465: ' + str(465 * x))
        result.append('line_466: ' + str(466 * x))
        result.append('line_467: ' + str(467 * x))
        result.append('line_468: ' + str(468 * x))
        result.append('line_469: ' + str(469 * x))
        result.append('line_470: ' + str(470 * x))
        result.append('line_471: ' + str(471 * x))
        result.append('line_472: ' + str(472 * x))
        result.append('line_473: ' + str(473 * x))
        result.append('line_474: ' + str(474 * x))
        result.append('line_475: ' + str(475 * x))
        result.append('line_476: ' + str(476 * x))
        result.append('line_477: ' + str(477 * x))
        result.append('line_478: ' + str(478 * x))
        result.append('line_479: ' + str(479 * x))
        result.append('line_480: ' + str(480 * x))
        result.append('line_481: ' + str(481 * x))
        result.append('line_482: ' + str(482 * x))
        result.append('line_483: ' + str(483 * x))
        result.append('line_484: ' + str(484 * x))
        result.append('line_485: ' + str(485 * x))
        result.append('line_486: ' + str(486 * x))
        result.append('line_487: ' + str(487 * x))
        result.append('line_488: ' + str(488 * x))
        result.append('line_489: ' + str(489 * x))
        result.append('line_490: ' + str(490 * x))
        result.append('line_491: ' + str(491 * x))
        result.append('line_492: ' + str(492 * x))
        result.append('line_493: ' + str(493 * x))
        result.append('line_494: ' + str(494 * x))
        result.append('line_495: ' + str(495 * x))
        result.append('line_496: ' + str(496 * x))
        result.append('line_497: ' + str(497 * x))
        result.append('line_498: ' + str(498 * x))
        result.append('line_499: ' + str(499 * x))
        result.append('line_500: ' + str(500 * x))
        result.append('line_501: ' + str(501 * x))
        result.append('line_502: ' + str(502 * x))
        result.append('line_503: ' + str(503 * x))
        result.append('line_504: ' + str(504 * x))
        result.append('line_505: ' + str(505 * x))
        result.append('line_506: ' + str(506 * x))
        result.append('line_507: ' + str(507 * x))
        result.append('line_508: ' + str(508 * x))
        result.append('line_509: ' + str(509 * x))
        result.append('line_510: ' + str(510 * x))
        result.append('line_511: ' + str(511 * x))
        result.append('line_512: ' + str(512 * x))
        result.append('line_513: ' + str(513 * x))
        result.append('line_514: ' + str(514 * x))
        result.append('line_515: ' + str(515 * x))
        result.append('line_516: ' + str(516 * x))
        result.append('line_517: ' + str(517 * x))
        result.append('line_518: ' + str(518 * x))
        result.append('line_519: ' + str(519 * x))
        result.append('line_520: ' + str(520 * x))
        result.append('line_521: ' + str(521 * x))
        result.append('line_522: ' + str(522 * x))
        result.append('line_523: ' + str(523 * x))
        result.append('line_524: ' + str(524 * x))
        result.append('line_525: ' + str(525 * x))
        result.append('line_526: ' + str(526 * x))
        result.append('line_527: ' + str(527 * x))
        result.append('line_528: ' + str(528 * x))
        result.append('line_529: ' + str(529 * x))
        result.append('line_530: ' + str(530 * x))
        result.append('line_531: ' + str(531 * x))
        result.append('line_532: ' + str(532 * x))
        result.append('line_533: ' + str(533 * x))
        result.append('line_534: ' + str(534 * x))
        result.append('line_535: ' + str(535 * x))
        result.append('line_536: ' + str(536 * x))
        result.append('line_537: ' + str(537 * x))
        result.append('line_538: ' + str(538 * x))
        result.append('line_539: ' + str(539 * x))
        result.append('line_540: ' + str(540 * x))
        result.append('line_541: ' + str(541 * x))
        result.append('line_542: ' + str(542 * x))
        result.append('line_543: ' + str(543 * x))
        result.append('line_544: ' + str(544 * x))
        result.append('line_545: ' + str(545 * x))
        result.append('line_546: ' + str(546 * x))
        result.append('line_547: ' + str(547 * x))
        result.append('line_548: ' + str(548 * x))
        result.append('line_549: ' + str(549 * x))
        result.append('line_550: ' + str(550 * x))
        result.append('line_551: ' + str(551 * x))
        result.append('line_552: ' + str(552 * x))
        result.append('line_553: ' + str(553 * x))
        result.append('line_554: ' + str(554 * x))
        result.append('line_555: ' + str(555 * x))
        result.append('line_556: ' + str(556 * x))
        result.append('line_557: ' + str(557 * x))
        result.append('line_558: ' + str(558 * x))
        result.append('line_559: ' + str(559 * x))
        result.append('line_560: ' + str(560 * x))
        result.append('line_561: ' + str(561 * x))
        result.append('line_562: ' + str(562 * x))
        result.append('line_563: ' + str(563 * x))
        result.append('line_564: ' + str(564 * x))
        result.append('line_565: ' + str(565 * x))
        result.append('line_566: ' + str(566 * x))
        result.append('line_567: ' + str(567 * x))
        result.append('line_568: ' + str(568 * x))
        result.append('line_569: ' + str(569 * x))
        result.append('line_570: ' + str(570 * x))
        result.append('line_571: ' + str(571 * x))
        result.append('line_572: ' + str(572 * x))
        result.append('line_573: ' + str(573 * x))
        result.append('line_574: ' + str(574 * x))
        result.append('line_575: ' + str(575 * x))
        result.append('line_576: ' + str(576 * x))
        result.append('line_577: ' + str(577 * x))
        result.append('line_578: ' + str(578 * x))
        result.append('line_579: ' + str(579 * x))
        result.append('line_580: ' + str(580 * x))
        result.append('line_581: ' + str(581 * x))
        result.append('line_582: ' + str(582 * x))
        result.append('line_583: ' + str(583 * x))
        result.append('line_584: ' + str(584 * x))
        result.append('line_585: ' + str(585 * x))
        result.append('line_586: ' + str(586 * x))
        result.append('line_587: ' + str(587 * x))
        result.append('line_588: ' + str(588 * x))
        result.append('line_589: ' + str(589 * x))
        result.append('line_590: ' + str(590 * x))
        result.append('line_591: ' + str(591 * x))
        result.append('line_592: ' + str(592 * x))
        result.append('line_593: ' + str(593 * x))
        result.append('line_594: ' + str(594 * x))
        result.append('line_595: ' + str(595 * x))
        result.append('line_596: ' + str(596 * x))
        result.append('line_597: ' + str(597 * x))
        result.append('line_598: ' + str(598 * x))
        result.append('line_599: ' + str(599 * x))
        result.append('line_600: ' + str(600 * x))
        result.append('line_601: ' + str(601 * x))
        result.append('line_602: ' + str(602 * x))
        result.append('line_603: ' + str(603 * x))
        result.append('line_604: ' + str(604 * x))
        result.append('line_605: ' + str(605 * x))
        result.append('line_606: ' + str(606 * x))
        result.append('line_607: ' + str(607 * x))
        result.append('line_608: ' + str(608 * x))
        result.append('line_609: ' + str(609 * x))
        result.append('line_610: ' + str(610 * x))
        result.append('line_611: ' + str(611 * x))
        result.append('line_612: ' + str(612 * x))
        result.append('line_613: ' + str(613 * x))
        result.append('line_614: ' + str(614 * x))
        result.append('line_615: ' + str(615 * x))
        result.append('line_616: ' + str(616 * x))
        result.append('line_617: ' + str(617 * x))
        result.append('line_618: ' + str(618 * x))
        result.append('line_619: ' + str(619 * x))
        result.append('line_620: ' + str(620 * x))
        result.append('line_621: ' + str(621 * x))
        result.append('line_622: ' + str(622 * x))
        result.append('line_623: ' + str(623 * x))
        result.append('line_624: ' + str(624 * x))
        result.append('line_625: ' + str(625 * x))
        result.append('line_626: ' + str(626 * x))
        result.append('line_627: ' + str(627 * x))
        result.append('line_628: ' + str(628 * x))
        result.append('line_629: ' + str(629 * x))
        result.append('line_630: ' + str(630 * x))
        result.append('line_631: ' + str(631 * x))
        result.append('line_632: ' + str(632 * x))
        result.append('line_633: ' + str(633 * x))
        result.append('line_634: ' + str(634 * x))
        result.append('line_635: ' + str(635 * x))
        result.append('line_636: ' + str(636 * x))
        result.append('line_637: ' + str(637 * x))
        result.append('line_638: ' + str(638 * x))
        result.append('line_639: ' + str(639 * x))
        result.append('line_640: ' + str(640 * x))
        result.append('line_641: ' + str(641 * x))
        result.append('line_642: ' + str(642 * x))
        result.append('line_643: ' + str(643 * x))
        result.append('line_644: ' + str(644 * x))
        result.append('line_645: ' + str(645 * x))
        result.append('line_646: ' + str(646 * x))
        result.append('line_647: ' + str(647 * x))
        result.append('line_648: ' + str(648 * x))
        result.append('line_649: ' + str(649 * x))
        result.append('line_650: ' + str(650 * x))
        result.append('line_651: ' + str(651 * x))
        result.append('line_652: ' + str(652 * x))
        result.append('line_653: ' + str(653 * x))
        result.append('line_654: ' + str(654 * x))
        result.append('line_655: ' + str(655 * x))
        result.append('line_656: ' + str(656 * x))
        result.append('line_657: ' + str(657 * x))
        result.append('line_658: ' + str(658 * x))
        result.append('line_659: ' + str(659 * x))
        result.append('line_660: ' + str(660 * x))
        result.append('line_661: ' + str(661 * x))
        result.append('line_662: ' + str(662 * x))
        result.append('line_663: ' + str(663 * x))
        result.append('line_664: ' + str(664 * x))
        result.append('line_665: ' + str(665 * x))
        result.append('line_666: ' + str(666 * x))
        result.append('line_667: ' + str(667 * x))
        result.append('line_668: ' + str(668 * x))
        result.append('line_669: ' + str(669 * x))
        result.append('line_670: ' + str(670 * x))
        result.append('line_671: ' + str(671 * x))
        result.append('line_672: ' + str(672 * x))
        result.append('line_673: ' + str(673 * x))
        result.append('line_674: ' + str(674 * x))
        result.append('line_675: ' + str(675 * x))
        result.append('line_676: ' + str(676 * x))
        result.append('line_677: ' + str(677 * x))
        result.append('line_678: ' + str(678 * x))
        result.append('line_679: ' + str(679 * x))
        result.append('line_680: ' + str(680 * x))
        result.append('line_681: ' + str(681 * x))
        result.append('line_682: ' + str(682 * x))
        result.append('line_683: ' + str(683 * x))
        result.append('line_684: ' + str(684 * x))
        result.append('line_685: ' + str(685 * x))
        result.append('line_686: ' + str(686 * x))
        result.append('line_687: ' + str(687 * x))
        result.append('line_688: ' + str(688 * x))
        result.append('line_689: ' + str(689 * x))
        result.append('line_690: ' + str(690 * x))
        result.append('line_691: ' + str(691 * x))
        result.append('line_692: ' + str(692 * x))
        result.append('line_693: ' + str(693 * x))
        result.append('line_694: ' + str(694 * x))
        result.append('line_695: ' + str(695 * x))
        result.append('line_696: ' + str(696 * x))
        result.append('line_697: ' + str(697 * x))
        result.append('line_698: ' + str(698 * x))
        result.append('line_699: ' + str(699 * x))
        result.append('line_700: ' + str(700 * x))
        result.append('line_701: ' + str(701 * x))
        result.append('line_702: ' + str(702 * x))
        result.append('line_703: ' + str(703 * x))
        result.append('line_704: ' + str(704 * x))
        result.append('line_705: ' + str(705 * x))
        result.append('line_706: ' + str(706 * x))
        result.append('line_707: ' + str(707 * x))
        result.append('line_708: ' + str(708 * x))
        result.append('line_709: ' + str(709 * x))
        result.append('line_710: ' + str(710 * x))
        result.append('line_711: ' + str(711 * x))
        result.append('line_712: ' + str(712 * x))
        result.append('line_713: ' + str(713 * x))
        result.append('line_714: ' + str(714 * x))
        result.append('line_715: ' + str(715 * x))
        result.append('line_716: ' + str(716 * x))
        result.append('line_717: ' + str(717 * x))
        result.append('line_718: ' + str(718 * x))
        result.append('line_719: ' + str(719 * x))
        result.append('line_720: ' + str(720 * x))
        result.append('line_721: ' + str(721 * x))
        result.append('line_722: ' + str(722 * x))
        result.append('line_723: ' + str(723 * x))
        result.append('line_724: ' + str(724 * x))
        result.append('line_725: ' + str(725 * x))
        result.append('line_726: ' + str(726 * x))
        result.append('line_727: ' + str(727 * x))
        result.append('line_728: ' + str(728 * x))
        result.append('line_729: ' + str(729 * x))
        result.append('line_730: ' + str(730 * x))
        result.append('line_731: ' + str(731 * x))
        result.append('line_732: ' + str(732 * x))
        result.append('line_733: ' + str(733 * x))
        result.append('line_734: ' + str(734 * x))
        result.append('line_735: ' + str(735 * x))
        result.append('line_736: ' + str(736 * x))
        result.append('line_737: ' + str(737 * x))
        result.append('line_738: ' + str(738 * x))
        result.append('line_739: ' + str(739 * x))
        result.append('line_740: ' + str(740 * x))
        result.append('line_741: ' + str(741 * x))
        result.append('line_742: ' + str(742 * x))
        result.append('line_743: ' + str(743 * x))
        result.append('line_744: ' + str(744 * x))
        result.append('line_745: ' + str(745 * x))
        result.append('line_746: ' + str(746 * x))
        result.append('line_747: ' + str(747 * x))
        result.append('line_748: ' + str(748 * x))
        result.append('line_749: ' + str(749 * x))
        result.append('line_750: ' + str(750 * x))
        result.append('line_751: ' + str(751 * x))
        result.append('line_752: ' + str(752 * x))
        result.append('line_753: ' + str(753 * x))
        result.append('line_754: ' + str(754 * x))
        result.append('line_755: ' + str(755 * x))
        result.append('line_756: ' + str(756 * x))
        result.append('line_757: ' + str(757 * x))
        result.append('line_758: ' + str(758 * x))
        result.append('line_759: ' + str(759 * x))
        result.append('line_760: ' + str(760 * x))
        result.append('line_761: ' + str(761 * x))
        result.append('line_762: ' + str(762 * x))
        result.append('line_763: ' + str(763 * x))
        result.append('line_764: ' + str(764 * x))
        result.append('line_765: ' + str(765 * x))
        result.append('line_766: ' + str(766 * x))
        result.append('line_767: ' + str(767 * x))
        result.append('line_768: ' + str(768 * x))
        result.append('line_769: ' + str(769 * x))
        result.append('line_770: ' + str(770 * x))
        result.append('line_771: ' + str(771 * x))
        result.append('line_772: ' + str(772 * x))
        result.append('line_773: ' + str(773 * x))
        result.append('line_774: ' + str(774 * x))
        result.append('line_775: ' + str(775 * x))
        result.append('line_776: ' + str(776 * x))
        result.append('line_777: ' + str(777 * x))
        result.append('line_778: ' + str(778 * x))
        result.append('line_779: ' + str(779 * x))
        result.append('line_780: ' + str(780 * x))
        result.append('line_781: ' + str(781 * x))
        result.append('line_782: ' + str(782 * x))
        result.append('line_783: ' + str(783 * x))
        result.append('line_784: ' + str(784 * x))
        result.append('line_785: ' + str(785 * x))
        result.append('line_786: ' + str(786 * x))
        result.append('line_787: ' + str(787 * x))
        result.append('line_788: ' + str(788 * x))
        result.append('line_789: ' + str(789 * x))
        result.append('line_790: ' + str(790 * x))
        result.append('line_791: ' + str(791 * x))
        result.append('line_792: ' + str(792 * x))
        result.append('line_793: ' + str(793 * x))
        result.append('line_794: ' + str(794 * x))
        result.append('line_795: ' + str(795 * x))
        result.append('line_796: ' + str(796 * x))
        result.append('line_797: ' + str(797 * x))
        result.append('line_798: ' + str(798 * x))
        result.append('line_799: ' + str(799 * x))
        result.append('line_800: ' + str(800 * x))
        result.append('line_801: ' + str(801 * x))
        result.append('line_802: ' + str(802 * x))
        result.append('line_803: ' + str(803 * x))
        result.append('line_804: ' + str(804 * x))
        result.append('line_805: ' + str(805 * x))
        result.append('line_806: ' + str(806 * x))
        result.append('line_807: ' + str(807 * x))
        result.append('line_808: ' + str(808 * x))
        result.append('line_809: ' + str(809 * x))
        result.append('line_810: ' + str(810 * x))
        result.append('line_811: ' + str(811 * x))
        result.append('line_812: ' + str(812 * x))
        result.append('line_813: ' + str(813 * x))
        result.append('line_814: ' + str(814 * x))
        result.append('line_815: ' + str(815 * x))
        result.append('line_816: ' + str(816 * x))
        result.append('line_817: ' + str(817 * x))
        result.append('line_818: ' + str(818 * x))
        result.append('line_819: ' + str(819 * x))
        result.append('line_820: ' + str(820 * x))
        result.append('line_821: ' + str(821 * x))
        result.append('line_822: ' + str(822 * x))
        result.append('line_823: ' + str(823 * x))
        result.append('line_824: ' + str(824 * x))
        result.append('line_825: ' + str(825 * x))
        result.append('line_826: ' + str(826 * x))
        result.append('line_827: ' + str(827 * x))
        result.append('line_828: ' + str(828 * x))
        result.append('line_829: ' + str(829 * x))
        result.append('line_830: ' + str(830 * x))
        result.append('line_831: ' + str(831 * x))
        result.append('line_832: ' + str(832 * x))
        result.append('line_833: ' + str(833 * x))
        result.append('line_834: ' + str(834 * x))
        result.append('line_835: ' + str(835 * x))
        result.append('line_836: ' + str(836 * x))
        result.append('line_837: ' + str(837 * x))
        result.append('line_838: ' + str(838 * x))
        result.append('line_839: ' + str(839 * x))
        result.append('line_840: ' + str(840 * x))
        result.append('line_841: ' + str(841 * x))
        result.append('line_842: ' + str(842 * x))
        result.append('line_843: ' + str(843 * x))
        result.append('line_844: ' + str(844 * x))
        result.append('line_845: ' + str(845 * x))
        result.append('line_846: ' + str(846 * x))
        result.append('line_847: ' + str(847 * x))
        result.append('line_848: ' + str(848 * x))
        result.append('line_849: ' + str(849 * x))
        result.append('line_850: ' + str(850 * x))
        result.append('line_851: ' + str(851 * x))
        result.append('line_852: ' + str(852 * x))
        result.append('line_853: ' + str(853 * x))
        result.append('line_854: ' + str(854 * x))
        result.append('line_855: ' + str(855 * x))
        result.append('line_856: ' + str(856 * x))
        result.append('line_857: ' + str(857 * x))
        result.append('line_858: ' + str(858 * x))
        result.append('line_859: ' + str(859 * x))
        result.append('line_860: ' + str(860 * x))
        result.append('line_861: ' + str(861 * x))
        result.append('line_862: ' + str(862 * x))
        result.append('line_863: ' + str(863 * x))
        result.append('line_864: ' + str(864 * x))
        result.append('line_865: ' + str(865 * x))
        result.append('line_866: ' + str(866 * x))
        result.append('line_867: ' + str(867 * x))
        result.append('line_868: ' + str(868 * x))
        result.append('line_869: ' + str(869 * x))
        result.append('line_870: ' + str(870 * x))
        result.append('line_871: ' + str(871 * x))
        result.append('line_872: ' + str(872 * x))
        result.append('line_873: ' + str(873 * x))
        result.append('line_874: ' + str(874 * x))
        result.append('line_875: ' + str(875 * x))
        result.append('line_876: ' + str(876 * x))
        result.append('line_877: ' + str(877 * x))
        result.append('line_878: ' + str(878 * x))
        result.append('line_879: ' + str(879 * x))
        result.append('line_880: ' + str(880 * x))
        result.append('line_881: ' + str(881 * x))
        result.append('line_882: ' + str(882 * x))
        result.append('line_883: ' + str(883 * x))
        result.append('line_884: ' + str(884 * x))
        result.append('line_885: ' + str(885 * x))
        result.append('line_886: ' + str(886 * x))
        result.append('line_887: ' + str(887 * x))
        result.append('line_888: ' + str(888 * x))
        result.append('line_889: ' + str(889 * x))
        result.append('line_890: ' + str(890 * x))
        result.append('line_891: ' + str(891 * x))
        result.append('line_892: ' + str(892 * x))
        result.append('line_893: ' + str(893 * x))
        result.append('line_894: ' + str(894 * x))
        result.append('line_895: ' + str(895 * x))
        result.append('line_896: ' + str(896 * x))
        result.append('line_897: ' + str(897 * x))
        result.append('line_898: ' + str(898 * x))
        result.append('line_899: ' + str(899 * x))
        result.append('line_900: ' + str(900 * x))
        result.append('line_901: ' + str(901 * x))
        result.append('line_902: ' + str(902 * x))
        result.append('line_903: ' + str(903 * x))
        result.append('line_904: ' + str(904 * x))
        result.append('line_905: ' + str(905 * x))
        result.append('line_906: ' + str(906 * x))
        result.append('line_907: ' + str(907 * x))
        result.append('line_908: ' + str(908 * x))
        result.append('line_909: ' + str(909 * x))
        result.append('line_910: ' + str(910 * x))
        result.append('line_911: ' + str(911 * x))
        result.append('line_912: ' + str(912 * x))
        result.append('line_913: ' + str(913 * x))
        result.append('line_914: ' + str(914 * x))
        result.append('line_915: ' + str(915 * x))
        result.append('line_916: ' + str(916 * x))
        result.append('line_917: ' + str(917 * x))
        result.append('line_918: ' + str(918 * x))
        result.append('line_919: ' + str(919 * x))
        result.append('line_920: ' + str(920 * x))
        result.append('line_921: ' + str(921 * x))
        result.append('line_922: ' + str(922 * x))
        result.append('line_923: ' + str(923 * x))
        result.append('line_924: ' + str(924 * x))
        result.append('line_925: ' + str(925 * x))
        result.append('line_926: ' + str(926 * x))
        result.append('line_927: ' + str(927 * x))
        result.append('line_928: ' + str(928 * x))
        result.append('line_929: ' + str(929 * x))
        result.append('line_930: ' + str(930 * x))
        result.append('line_931: ' + str(931 * x))
        result.append('line_932: ' + str(932 * x))
        result.append('line_933: ' + str(933 * x))
        result.append('line_934: ' + str(934 * x))
        result.append('line_935: ' + str(935 * x))
        result.append('line_936: ' + str(936 * x))
        result.append('line_937: ' + str(937 * x))
        result.append('line_938: ' + str(938 * x))
        result.append('line_939: ' + str(939 * x))
        result.append('line_940: ' + str(940 * x))
        result.append('line_941: ' + str(941 * x))
        result.append('line_942: ' + str(942 * x))
        result.append('line_943: ' + str(943 * x))
        result.append('line_944: ' + str(944 * x))
        result.append('line_945: ' + str(945 * x))
        result.append('line_946: ' + str(946 * x))
        result.append('line_947: ' + str(947 * x))
        result.append('line_948: ' + str(948 * x))
        result.append('line_949: ' + str(949 * x))
        result.append('line_950: ' + str(950 * x))
        result.append('line_951: ' + str(951 * x))
        result.append('line_952: ' + str(952 * x))
        result.append('line_953: ' + str(953 * x))
        result.append('line_954: ' + str(954 * x))
        result.append('line_955: ' + str(955 * x))
        result.append('line_956: ' + str(956 * x))
        result.append('line_957: ' + str(957 * x))
        result.append('line_958: ' + str(958 * x))
        result.append('line_959: ' + str(959 * x))
        result.append('line_960: ' + str(960 * x))
        result.append('line_961: ' + str(961 * x))
        result.append('line_962: ' + str(962 * x))
        result.append('line_963: ' + str(963 * x))
        result.append('line_964: ' + str(964 * x))
        result.append('line_965: ' + str(965 * x))
        result.append('line_966: ' + str(966 * x))
        result.append('line_967: ' + str(967 * x))
        result.append('line_968: ' + str(968 * x))
        result.append('line_969: ' + str(969 * x))
        result.append('line_970: ' + str(970 * x))
        result.append('line_971: ' + str(971 * x))
        result.append('line_972: ' + str(972 * x))
        result.append('line_973: ' + str(973 * x))
        result.append('line_974: ' + str(974 * x))
        result.append('line_975: ' + str(975 * x))
        result.append('line_976: ' + str(976 * x))
        result.append('line_977: ' + str(977 * x))
        result.append('line_978: ' + str(978 * x))
        result.append('line_979: ' + str(979 * x))
        result.append('line_980: ' + str(980 * x))
        result.append('line_981: ' + str(981 * x))
        result.append('line_982: ' + str(982 * x))
        result.append('line_983: ' + str(983 * x))
        result.append('line_984: ' + str(984 * x))
        result.append('line_985: ' + str(985 * x))
        result.append('line_986: ' + str(986 * x))
        result.append('line_987: ' + str(987 * x))
        result.append('line_988: ' + str(988 * x))
        result.append('line_989: ' + str(989 * x))
        return result
