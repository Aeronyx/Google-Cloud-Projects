from bs4 import BeautifulSoup
from statistics import mean, median, quantiles
from google.cloud import storage

links_dict = {}
outgoing_links_count = {}
incoming_links_count = {}

def extract_outgoing_links(content, filename):
    soup = BeautifulSoup(content, features="html.parser")
    counter = 0
    outgoing = []
    for link in soup.findAll('a'):
        outgoing.append(link.get('href'))
        counter += 1
    outgoing_links_count[filename] = counter
    return outgoing

def read_files():
    client = storage.Client()
    blobs = client.list_blobs('ds561-hw2-html-linked-bucket', prefix='html/')

    for blob in blobs:
        filename = blob.name[5:]
        
        with blob.open("r") as f: 
            data = f.read()
            outgoing = extract_outgoing_links(data, filename)
            links_dict[filename] = outgoing
            
def pagerank(data, max_iter=20, tol=.005):
    N = len(data)
    PR = {}
    
    # initialize
    for pagename in list(data.keys()):
        PR[pagename] = 1/N
    
    for i in range(max_iter):
        print(f"- Iteration {i}...")
        PR_last = PR.copy()
        
        for rank in list(PR.keys()):
            temp = []
            for name in list(data.keys()):
                if rank in data[name]:
                    temp.append(name)
            incoming_links_count[rank] = len(temp)
            try:
                PR[rank] = .15 + .85 * sum(PR[linked_page]/len(data[linked_page]) for linked_page in temp)
            except(ZeroDivisionError):
                continue
        
        if abs(sum(PR[i] for i in list(data.keys()))-sum(PR_last[i] for i in list(data.keys()))) <= tol * sum(PR_last[i] for i in list(data.keys())):
            print(f"Converged in {i+1} iterations")
            break
            
    return PR

def run_PR():
    final = pagerank(links_dict)
    top5 = sorted(final, key=final.get, reverse=True)[:5]
    print("\nTop 5 Pages:")
    counter = 0
    for i in top5:
        counter += 1
        print("Page #"+str(counter)+":", i, "| Rank:", round(final[i], 6))

def stats():
    inc_counts = list(incoming_links_count.values())
    outg_counts = list(outgoing_links_count.values())
    inc_mean = mean(inc_counts)
    outg_mean = mean(outg_counts)
    inc_med = median(inc_counts)
    outg_med = median(outg_counts)
    inc_max = max(inc_counts)
    inc_max_filename = max(zip(incoming_links_count.values(), incoming_links_count.keys()))[1]
    outg_max = max(outg_counts)
    outg_max_filename = max(zip(outgoing_links_count.values(), outgoing_links_count.keys()))[1]
    inc_min = min(inc_counts)
    inc_min_filename = min(zip(incoming_links_count.values(), incoming_links_count.keys()))[1]
    outg_min = min(outg_counts)
    outg_min_filename = min(zip(outgoing_links_count.values(), outgoing_links_count.keys()))[1]
    inc_quintiles = quantiles(inc_counts, n=6)
    outg_quintiles = quantiles(outg_counts, n=6)

    print("\nIncoming links:") 
    print("Mean:", inc_mean)
    print("Median:", inc_med)
    print("Max:", inc_max, "from file", inc_max_filename)
    print("Min:", inc_min, "from file", inc_min_filename)
    print("Quintiles:", inc_quintiles)

    print("\nOutgoing links:") 
    print("Mean:", outg_mean)
    print("Median:", outg_med)
    print("Max:", outg_max, "from file", outg_max_filename)
    print("Min:", outg_min, "from file", outg_min_filename)
    print("Quintiles:", outg_quintiles)
    
def main():
    print("Reading files...")
    read_files()
    print("Finished reading files.")
    print("Running pagerank...")
    print("\nPAGERANK")
    print("------------------------------")
    run_PR()
    print("------------------------------")
    print("\nSTATISTICS")
    print("------------------------------")
    stats()
    print("------------------------------")
    
main()