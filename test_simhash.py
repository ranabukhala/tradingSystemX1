import sys
sys.path.insert(0, "/app")
from app.pipeline.simhash import compute_simhash, hamming_distance

titles = [
    "Nebius shares pop on 27M AI infrastructure deal with Meta Platforms",
    "Meta stock pops on planned layoffs 27 billion Nebius cloud-computing deal",
    "Meta and Nebius AI deal Micron new chip plant Dollar Tree guidance",
    "Nebius Meta AI Deal Raises New Questions On Scale And Risk",
    "Meta Stock Rises Wall Street Sees Layoffs Report As Broader Shift In AI Narrative",
    "Meta shares rise amid reports of potential 20 percent workforce cut",
]

for i in range(len(titles)):
    for j in range(i+1, len(titles)):
        d = hamming_distance(compute_simhash(titles[i]), compute_simhash(titles[j]))
        label = "MATCH" if d <= 8 else "MISS"
        print("d=%2d %s [%d] vs [%d]  %s | %s" % (d, label, i, j, titles[i][:45], titles[j][:45]))