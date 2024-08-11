from enum import Enum
from typing import Literal


class ExtendedColorType(Enum):
    BLACK = 0
    MAROON = 1
    GREEN = 2
    OLIVE = 3
    NAVY = 4
    PURPLE = 5
    TEAL = 6
    SILVER = 7
    GREY = 8
    RED = 9
    LIME = 10
    YELLOW = 11
    BLUE = 12
    FUCHSIA = 13
    AQUA = 14
    WHITE = 15
    GREY_2 = 16
    NAVY_BLUE = 17
    DARK_BLUE = 18
    BLUE_2 = 19
    BLUE_3 = 20
    BLUE_4 = 21
    DARK_GREEN = 22
    DEEP_SKY_BLUE = 23
    DEEP_SKY_BLUE_1 = 24
    DEEP_SKY_BLUE_2 = 25
    DODGER_BLUE_4 = 26
    DODGER_BLUE_2 = 27
    GREEN_1 = 28
    SPRING_GREEN = 29
    TURQUOISE = 30
    DEEP_SKY_BLUE_4 = 31
    DEEP_SKY_BLUE_5 = 32
    DODGER_BLUE_3 = 33
    GREEN_2 = 34
    SPRING_GREEN_2 = 35
    DARK_CYAN = 36
    LIGHT_SEA_GREEN = 37
    DEEP_SKY_BLUE_6 = 38
    DEEP_SKY_BLUE_7 = 39
    GREEN_3 = 40
    SPRING_GREEN_3 = 41
    SPRING_GREEN_4 = 42
    CYAN = 43
    DARK_TURQUOISE = 44
    TURQUOISE_2 = 45
    GREEN_4 = 46
    SPRING_GREEN_5 = 47
    SPRING_GREEN_6 = 48
    MEDIUM_SPRING_GREEN = 49
    CYAN_2 = 50
    CYAN_3 = 51
    DARK_RED = 52
    DEEP_PINK = 53
    PURPLE_2 = 54
    PURPLE_3 = 55
    PURPLE_4 = 56
    BLUE_VIOLET = 57
    ORANGE = 58
    GREY_3 = 59
    MEDIUM_PURPLE = 60
    SLATE_BLUE = 61
    SLATE_BLUE_2 = 62
    ROYAL_BLUE = 63
    CHARTREUSE = 64
    DARK_SEA_GREEN = 65
    PALE_TURQUOISE = 66
    STEEL_BLUE = 67
    STEEL_BLUE_2 = 68
    CORNFLOWER_BLUE = 69
    CHARTREUSE_2 = 70
    DARK_SEA_GREEN_2 = 71
    CADET_BLUE = 72
    CADET_BLUE_2 = 73
    SKY_BLUE = 74
    STEEL_BLUE_3 = 75
    CHARTREUSE_3 = 76
    PALE_GREEN = 77
    SEA_GREEN = 78
    AQUAMARINE = 79
    MEDIUM_TURQUOISE = 80
    STEEL_BLUE_4 = 81
    CHARTREUSE_4 = 82
    SEA_GREEN_2 = 83
    SEA_GREEN_3 = 84
    AQUAMARINE_2 = 86
    DARK_SLATE_GRAY = 87
    DARK_RED_2 = 88
    DEEP_PINK_2 = 89
    DARK_MAGENTA = 90
    DARK_MAGENTA_2 = 91
    DARK_VIOLET = 92
    PURPLE_5 = 93
    ORANGE_2 = 94
    LIGHT_PINK = 95
    PLUM = 96
    MEDIUM_PURPLE_2 = 97
    MEDIUM_PURPLE_3 = 98
    SLATE_BLUE_3 = 99
    YELLOW_2 = 100
    WHEAT = 101
    GREY_4 = 102
    LIGHT_SLATE_GREY = 103
    MEDIM_PURPLE_4 = 104
    LIGHT_SLATE_BLUE = 105
    YELLOW_3 = 106
    DARK_OLIVE_GREEN = 107
    DARK_SEA_GREEN_3 = 108
    LIGHT_SKY_BLUE = 109
    LIGHT_SKY_BLUE_2 = 110
    SKY_BLUE_2 = 111
    CHARTREUSE_5 = 112
    DARK_OLIVE_GREEN_2 = 113
    PALE_GREEN_2 = 114
    DARK_SEA_GREEN_4 = 115
    DARK_SLATE_GRAY_2 = 116
    SKY_BLUE_3 = 117
    CHARTREUSE_6 = 118
    LIGHT_GREEN = 119
    LIGHT_GREEN_2 = 120
    PALE_GREEN_3 = 121
    AQUAMARINE_3 = 122
    DARK_SLATE_GRAY_3 = 123
    RED_2 = 124
    DEEP_PINK_3 = 125
    MEDIUM_VIOLET_RED = 126
    MAGENTA = 127
    DARK_VIOLET_2 = 128
    PURPLE_6 = 129
    DARK_ORANGE = 130
    INDIAN_RED = 131
    HOT_PINK = 132
    MEDIUM_ORCHID = 133
    MEDIUM_ORCHID_2 = 134
    MEDIUM_PURPLE_4 = 135
    DARK_GOLDENROD = 136
    LIGHT_SALMON = 137
    ROSY_BROWN = 138
    GREY_5 = 139
    MEDIUM_PURPLE_5 = 140
    MEDIUM_PURPLE_6 = 141
    GOLD = 142
    DARK_KHAKI = 143
    NAVAJO_WHITE = 144
    GREY_6 = 145
    LIGHT_STEEL_BLUE = 146
    LIGHT_STEEL_BLUE_2 = 147
    YELLOW_4 = 148
    DARK_OLIVE_GREEN_3 = 149
    DARK_SEA_GREEN_5 = 150
    DARK_SEA_GREEN_6 = 151
    LIGHT_CYAN = 152
    LIGHT_SKY_BLUE_3 = 153
    GREEN_YELLOW = 154
    DARK_OLIVE_GREEN_4 = 155
    PALE_GREEN_4 = 156
    DARK_SEA_GREEN_7 = 157
    DARK_SEA_GREEN_8 = 158
    PALE_TURQUOISE_2 = 159
    RED_3 = 160
    DEEP_PINK_4 = 161
    DEEP_PINK_5 = 162
    MAGENTA_2 = 163
    MAGENTA_3 = 164
    MAGENTA_4 = 165
    DARK_ORANGE_2 = 166
    INDIAN_RED_2 = 167
    HOT_PINK_2 = 168
    HOT_PINK_3 = 169
    ORCHID = 170
    MEDIUM_ORCHID_3 = 171
    ORANGE_3 = 172
    LIGHT_SALMON_2 = 173
    LIGHT_PINK_2 = 174
    PINK = 175
    PLUM_2 = 176
    VIOLET = 177
    GOLD_2 = 178
    LIGHT_GOLDENROD = 179
    TAN = 180
    MISTY_ROSE = 181
    THISTLE = 182
    PLUM_3 = 183
    YELLOW_5 = 184
    KHAKI = 185
    LIGHT_GOLDENROD_2 = 186
    LIGHT_YELLOW = 187
    GREY_7 = 188
    LIGHT_STEEL_BLUE_3 = 189
    YELLOW_6 = 190
    DARK_OLIVE_GREEN_5 = 191
    DARK_OLIVE_GREEN_6 = 192
    DARK_SEA_GREEN_9 = 193
    HONEYDEW = 194
    LIGHT_CYAN_2 = 195
    RED_4 = 196
    DEEP_PINK_6 = 197
    DEEP_PINK_7 = 197
    DEEP_PINK_8 = 198
    DEEP_PINK_9 = 199
    MAGENTA_5 = 200
    MAGENTA_6 = 201
    ORANGE_RED = 202
    INDIAN_RED_3 = 203
    INDIAN_RED_4 = 204
    HOT_PINK_4 = 205
    HOT_PINK_5 = 206
    MEDIUM_ORCHID_4 = 207
    DARK_ORANGE_3 = 208
    SALMON = 209
    LIGHT_CORAL = 210
    PALE_VIOLET_RED = 211
    ORCHID_2 = 212
    ORCHIRD_3 = 213
    ORCHID_4 = 214
    SANDY_BROWN = 215
    LIGHT_SALMON_3 = 216
    LIGHT_PINK_3 = 217
    PINK_2 = 218
    PLUM_4 = 219
    GOLD_3 = 220
    LIGHT_GOLDENROD_3 = 221
    LIGHT_GOLDENROD_4 = 222
    NAVAJO_WHITE_2 = 223
    MISTY_ROSE_2 = 224
    THISTLE_2 = 225
    YELLOW_7 = 226
    LIGHT_GOLDENROD_5 = 227
    KHAKI_2 = 228
    WHEAT_2 = 229
    CORNSILK = 230
    GREY_8 = 231
    GREY_9 = 232
    GREY_10 = 233
    GREY_11 = 234
    GREY_12 = 235
    GREY_13 = 236
    GREY_14 = 237
    GREY_15 = 238
    GREY_16 = 239
    GREY_17 = 240
    GREY_18 = 241
    GREY_19 = 242
    GREY_20 = 243
    GREY_21 = 244
    GREY_22 = 245
    GREY_23 = 246
    GREY_24 = 247
    GREY_25 = 248
    GREY_26 = 249
    GREY_27 = 250
    GREY_28 = 251
    GREY_29 = 252
    GREY_30 = 253
    GREY_31 = 254
    GREY_32 = 255


ExtendedColorName = Literal[
    "black",
    "maroon",
    "green",
    "olive",
    "navy",
    "purple",
    "teal ",
    "silver",
    "grey",
    "red",
    "lime",
    "yellow",
    "blue",
    "fuchsia",
    "aqua",
    "white",
    "grey_2",
    "navy_blue",
    "dark_blue",
    "blue_2",
    "blue_3",
    "blue_4",
    "dark_green",
    "deep_sky_blue",
    "deep_sky_blue_1",
    "deep_sky_blue_2",
    "dodger_blue_4",
    "dodger_blue_2",
    "green_1",
    "spring_green",
    "turquoise",
    "deep_sky_blue_4",
    "deep_sky_blue_5",
    "dodger_blue_3",
    "green_2",
    "spring_green_2",
    "dark_cyan",
    "light_sea_green",
    "deep_sky_blue_6 ",
    "deep_sky_blue_7",
    "green_3",
    "spring_green_3",
    "spring_green_4",
    "cyan",
    "dark_turquoise",
    "turquoise_2",
    "green_4",
    "spring_green_5",
    "spring_green_6",
    "medium_spring_green",
    "cyan_2",
    "cyan_3",
    "dark_red",
    "deep_pink",
    "purple_2",
    "purple_3",
    "purple_4",
    "blue_violet",
    "orange",
    "grey_3",
    "medium_purple",
    "slate_blue",
    "slate_blue_2",
    "royal_blue",
    "chartreuse",
    "dark_sea_green",
    "pale_turquoise",
    "steel_blue",
    "steel_blue_2",
    "cornflower_blue",
    "chartreuse_2",
    "dark_sea_green_2",
    "cadet_blue",
    "cadet_blue_2",
    "sky_blue",
    "steel_blue_3",
    "chartreuse_3",
    "pale_green",
    "sea_green",
    "aquamarine",
    "medium_turquoise",
    "steel_blue_4",
    "chartreuse_4",
    "sea_green_2",
    "sea_green_3",
    "aquamarine_2",
    "dark_slate_gray",
    "dark_red_2",
    "deep_pink_2",
    "dark_magenta",
    "dark_magenta_2",
    "dark_violet",
    "purple_5",
    "orange_2",
    "light_pink",
    "plum",
    "medium_purple_2",
    "medium_purple_3",
    "slate_blue_3",
    "yellow_2",
    "wheat",
    "grey_4",
    "light_slate_grey",
    "medim_purple_4",
    "light_slate_blue",
    "yellow_3",
    "dark_olive_green",
    "dark_sea_green_3",
    "light_sky_blue",
    "light_sky_blue_2",
    "sky_blue_2",
    "chartreuse_5",
    "dark_olive_green_2",
    "pale_green_2",
    "dark_sea_green_4",
    "dark_slate_gray_2",
    "sky_blue_3",
    "chartreuse_6",
    "light_green",
    "light_green_2",
    "pale_green_3",
    "aquamarine_3",
    "dark_slate_gray_3",
    "red_2",
    "deep_pink_3",
    "medium_violet_red",
    "magenta",
    "dark_violet_2",
    "purple_6",
    "dark_orange",
    "indian_red",
    "hot_pink",
    "medium_orchid",
    "medium_orchid_2",
    "medium_purple_4",
    "dark_goldenrod",
    "light_salmon",
    "rosy_brown",
    "grey_5",
    "medium_purple_5",
    "medium_purple_6",
    "gold",
    "dark_khaki",
    "navajo_white",
    "grey_6",
    "light_steel_blue",
    "light_steel_blue_2",
    "yellow_4",
    "dark_olive_green_3",
    "dark_sea_green_5",
    "dark_sea_green_6",
    "light_cyan",
    "light_sky_blue_3",
    "green_yellow",
    "dark_olive_green_4",
    "pale_green_4",
    "dark_sea_green_7",
    "dark_sea_green_8",
    "pale_turquoise_2",
    "red_3",
    "deep_pink_4",
    "deep_pink_5",
    "magenta_2",
    "magenta_3",
    "magenta_4",
    "dark_orange_2",
    "indian_red_2",
    "hot_pink_2",
    "hot_pink_3",
    "orchid",
    "medium_orchid_3",
    "orange_3",
    "light_salmon_2",
    "light_pink_2",
    "pink",
    "plum_2",
    "violet",
    "gold_2",
    "light_goldenrod",
    "tan",
    "misty_rose",
    "thistle",
    "plum_3",
    "yellow_5",
    "khaki",
    "light_goldenrod_2",
    "light_yellow",
    "grey_7",
    "light_steel_blue_3",
    "yellow_6",
    "dark_olive_green_5",
    "dark_olive_green_6",
    "dark_sea_green_9",
    "honeydew",
    "light_cyan_2",
    "red_4",
    "deep_pink_6",
    "deep_pink_7",
    "deep_pink_8",
    "deep_pink_9",
    "magenta_5",
    "magenta_6",
    "orange_red",
    "indian_red_3",
    "indian_red_4",
    "hot_pink_4",
    "hot_pink_5",
    "medium_orchid_4",
    "dark_orange_3",
    "salmon",
    "light_coral",
    "pale_violet_red",
    "orchid_2",
    "orchird_3",
    "orchid_4",
    "sandy_brown",
    "light_salmon_3",
    "light_pink_3",
    "pink_2",
    "plum_4",
    "gold_3",
    "light_goldenrod_3",
    "light_goldenrod_4",
    "navajo_white_2",
    "misty_rose_2",
    "thistle_2",
    "yellow_7",
    "light_goldenrod_5",
    "khaki_2",
    "wheat_2",
    "cornsilk",
    "grey_8",
    "grey_9",
    "grey_10",
    "grey_11",
    "grey_12",
    "grey_13",
    "grey_14",
    "grey_15",
    "grey_16",
    "grey_17",
    "grey_18",
    "grey_19",
    "grey_20",
    "grey_21",
    "grey_22",
    "grey_23",
    "grey_24",
    "grey_25",
    "grey_26",
    "grey_27",
    "grey_28",
    "grey_29",
    "grey_30",
    "grey_31",
    "grey_32",
]
