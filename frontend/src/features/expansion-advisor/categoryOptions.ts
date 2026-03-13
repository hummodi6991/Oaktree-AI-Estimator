/**
 * Curated F&B category catalog for Riyadh Expansion Advisor.
 *
 * Each entry carries a normalised `value` (sent in the API payload),
 * user-facing English / Arabic labels, a browsing `group`, and
 * search `aliases` so users can find categories by common synonyms
 * in either language.
 */

export type CategoryOption = {
  value: string;
  label: string;
  label_ar: string;
  group: string;
  aliases: string[];
};

export type CategoryGroup = {
  key: string;
  label: string;
  label_ar: string;
};

export const CATEGORY_GROUPS: CategoryGroup[] = [
  { key: "quick_service", label: "Quick Service", label_ar: "خدمة سريعة" },
  { key: "cafe_dessert", label: "Café & Dessert", label_ar: "مقهى وحلويات" },
  { key: "casual_dining", label: "Casual Dining", label_ar: "مطاعم كاجوال" },
  { key: "traditional_regional", label: "Traditional / Regional", label_ar: "تقليدي / إقليمي" },
  { key: "international", label: "International Cuisines", label_ar: "مطابخ عالمية" },
  { key: "delivery_first", label: "Delivery-First Formats", label_ar: "توصيل أولاً" },
];

export const CATEGORY_OPTIONS: CategoryOption[] = [
  // ── Quick Service ──
  { value: "burger", label: "Burger", label_ar: "برغر", group: "quick_service", aliases: ["burgers", "burger restaurant", "برجر", "همبرغر", "hamburgini"] },
  { value: "fried chicken", label: "Fried Chicken", label_ar: "دجاج مقلي", group: "quick_service", aliases: ["fried chicken restaurant", "broasted", "بروستد", "فراخ", "دجاج"] },
  { value: "pizza", label: "Pizza", label_ar: "بيتزا", group: "quick_service", aliases: ["pizzas", "pizza restaurant", "بيتزا مطعم", "معجنات"] },
  { value: "shawarma", label: "Shawarma", label_ar: "شاورما", group: "quick_service", aliases: ["shawerma", "شاورما عربي", "شاورما سوري"] },
  { value: "sandwiches", label: "Sandwiches", label_ar: "ساندويتشات", group: "quick_service", aliases: ["sandwich", "subs", "ساندوتش", "سندويش", "سندوتشات"] },
  { value: "fast food", label: "Fast Food", label_ar: "وجبات سريعة", group: "quick_service", aliases: ["fastfood", "fast-food", "qsr", "اكل سريع"] },
  { value: "breakfast", label: "Breakfast", label_ar: "فطور", group: "quick_service", aliases: ["breakfast restaurant", "فطور صباحي", "إفطار", "افطار"] },

  // ── Café & Dessert ──
  { value: "cafe", label: "Café", label_ar: "مقهى", group: "cafe_dessert", aliases: ["coffee shop", "كوفي شوب", "كافيه", "قهوة", "مقاهي"] },
  { value: "coffee", label: "Coffee", label_ar: "قهوة", group: "cafe_dessert", aliases: ["specialty coffee", "قهوة مختصة", "كوفي", "محمصة"] },
  { value: "bakery", label: "Bakery", label_ar: "مخبز", group: "cafe_dessert", aliases: ["bakeries", "pastry", "مخبوزات", "معجنات", "فطائر", "patisserie"] },
  { value: "dessert", label: "Dessert", label_ar: "حلويات", group: "cafe_dessert", aliases: ["desserts", "sweets", "حلا", "حلى", "كيك", "cake"] },
  { value: "ice cream", label: "Ice Cream", label_ar: "آيس كريم", group: "cafe_dessert", aliases: ["gelato", "frozen yogurt", "ايسكريم", "بوظة", "جيلاتو"] },
  { value: "juice", label: "Juice", label_ar: "عصائر", group: "cafe_dessert", aliases: ["juices", "smoothie", "smoothies", "عصير", "سموذي"] },

  // ── Casual Dining ──
  { value: "casual dining", label: "Casual Dining", label_ar: "مطعم كاجوال", group: "casual_dining", aliases: ["casual restaurant", "مطاعم عائلية"] },
  { value: "family restaurant", label: "Family Restaurant", label_ar: "مطعم عائلي", group: "casual_dining", aliases: ["family dining", "مطاعم عائلية", "عائلي"] },
  { value: "fine dining", label: "Fine Dining", label_ar: "مطعم فاخر", group: "casual_dining", aliases: ["upscale dining", "fine restaurant", "مطاعم فاخرة", "فاين داينينق"] },
  { value: "steakhouse", label: "Steakhouse", label_ar: "ستيك هاوس", group: "casual_dining", aliases: ["steak", "steaks", "steak house", "ستيك", "لحوم"] },
  { value: "grills", label: "Grills", label_ar: "مشاوي", group: "casual_dining", aliases: ["grill", "grilled", "bbq", "barbecue", "مشوي", "شواء", "شوايات"] },
  { value: "seafood", label: "Seafood", label_ar: "مأكولات بحرية", group: "casual_dining", aliases: ["fish", "sea food", "سمك", "أسماك", "بحري"] },
  { value: "healthy", label: "Healthy", label_ar: "صحي", group: "casual_dining", aliases: ["health food", "healthy food", "اكل صحي", "دايت", "diet"] },
  { value: "salad", label: "Salad", label_ar: "سلطات", group: "casual_dining", aliases: ["salads", "salad bar", "سلطة"] },

  // ── Traditional / Regional ──
  { value: "traditional saudi", label: "Traditional Saudi", label_ar: "أكل سعودي", group: "traditional_regional", aliases: ["saudi", "saudi food", "سعودي", "كبسة", "kabsa", "مندي", "مطبخ سعودي"] },
  { value: "traditional arabic", label: "Traditional Arabic", label_ar: "أكل عربي", group: "traditional_regional", aliases: ["arabic", "arab food", "عربي", "مطبخ عربي"] },
  { value: "yemeni", label: "Yemeni", label_ar: "يمني", group: "traditional_regional", aliases: ["yemen", "yemen food", "مطعم يمني", "مندي يمني", "حنيذ"] },
  { value: "levantine", label: "Levantine", label_ar: "شامي", group: "traditional_regional", aliases: ["lebanese", "syrian", "لبناني", "سوري", "مطبخ شامي", "شاميات"] },

  // ── International Cuisines ──
  { value: "indian", label: "Indian", label_ar: "هندي", group: "international", aliases: ["india", "indian food", "مطعم هندي", "بريانيي", "biryani", "مطبخ هندي"] },
  { value: "pakistani", label: "Pakistani", label_ar: "باكستاني", group: "international", aliases: ["pakistan", "pakistani food", "مطعم باكستاني"] },
  { value: "turkish", label: "Turkish", label_ar: "تركي", group: "international", aliases: ["turkey", "turkish food", "مطعم تركي", "كباب تركي", "مطبخ تركي"] },
  { value: "italian", label: "Italian", label_ar: "إيطالي", group: "international", aliases: ["italy", "italian food", "pasta", "باستا", "مطعم إيطالي", "إيطالي", "ايطالي"] },
  { value: "chinese", label: "Chinese", label_ar: "صيني", group: "international", aliases: ["china", "chinese food", "مطعم صيني", "مطبخ صيني"] },
  { value: "japanese", label: "Japanese", label_ar: "ياباني", group: "international", aliases: ["japan", "japanese food", "مطعم ياباني", "رامن", "ramen"] },
  { value: "sushi", label: "Sushi", label_ar: "سوشي", group: "international", aliases: ["sushi restaurant", "سوشي مطعم"] },
  { value: "korean", label: "Korean", label_ar: "كوري", group: "international", aliases: ["korea", "korean food", "مطعم كوري", "كوري"] },
  { value: "mexican", label: "Mexican", label_ar: "مكسيكي", group: "international", aliases: ["mexico", "mexican food", "tacos", "تاكو", "مطعم مكسيكي"] },
  { value: "american", label: "American", label_ar: "أمريكي", group: "international", aliases: ["usa", "american food", "مطعم أمريكي", "امريكي"] },

  // ── Delivery-First Formats ──
  { value: "delivery kitchen", label: "Delivery Kitchen", label_ar: "مطبخ توصيل", group: "delivery_first", aliases: ["dark kitchen", "ghost kitchen", "مطبخ سحابي"] },
  { value: "cloud kitchen", label: "Cloud Kitchen", label_ar: "مطبخ سحابي", group: "delivery_first", aliases: ["virtual kitchen", "virtual restaurant", "كلاود كيتشن"] },
];

/**
 * Look up a category by value. Returns the matching option, or undefined
 * if the value is not in the curated list (legacy / free-text fallback).
 */
export function findCategoryOption(value: string): CategoryOption | undefined {
  const lower = value.trim().toLowerCase();
  return CATEGORY_OPTIONS.find((opt) => opt.value === lower);
}
