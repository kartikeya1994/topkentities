def getString(l):
	ans = l[0]
	for i in range(1,len(l)):
		ans += ' '+l[i]
	return ans
def getEntities(ll):
	ans = getString(ll[0])
	for i in range(1,len(ll)):
		ans += ','+getString(ll[i])
	return ans
def checkSub(l,m):
	llen = len(l)
	mlen = len(m)
	if llen > mlen:
		return False
	for i in range(mlen - llen +1):
		if l[0] == m[i]:
			for j in range(llen):
				if l[j] != m[j+i]:
					return False
			return True
	return False


lines = None
with open('dict.txt','r') as f:
	lines = f.readlines()
#lines = [line.split('=')[0].strip().split(' ') for line in lines]
# for i in range(len(lines)):
# 	lines[i] = [t.lower() for t in lines[i]]
#lines = lines[:1000]
groups = dict()
group_counts = dict()
sorted_list = ['']*len(lines)
i=0
for l in lines:
	e = l.split('=')[0].strip().lower().split(' ')
	e_key = getString(e)
	if e_key in groups:
		group_counts[e_key] += int(l.split('=')[1].strip())
	else:
		groups[e_key] = [e]
		group_counts[e_key] = int(l.split('=')[1].strip())
		sorted_list[i] = e_key
		#print groups[e_key], group_counts[e_key]
		i += 1

keys = sorted_list
for i in range(len(keys)):
	print i
	if keys[i] not in groups:
		continue
	for j in range(len(keys)):
		if i==j or keys[j] not in groups:
			continue
		# for k in range(len(groups[keys[j]])):
		# 	if checkSub(groups[keys[i]][0], groups[keys[j]][k]):
		# 		groups[keys[i]] += groups[keys[j]]
		# 		group_counts[keys[i]] += group_counts[keys[j]]
		# 		#print groups[keys[i]]
		# 		del groups[keys[j]]
		# 		break
		if checkSub(groups[keys[i]][0], groups[keys[j]][0]):
			groups[keys[i]] += groups[keys[j]]
			group_counts[keys[i]] += group_counts[keys[j]]
			#print groups[keys[i]]
			del groups[keys[j]]
			print group_counts['united states']
			break
		# elif checkSub(groups[keys[j]][0], groups[keys[i]][0]):
		# 	groups[keys[j]] += groups[keys[i]]
		# 	group_counts[keys[j]] += group_counts[keys[i]]
		# 	#print groups[keys[i]]
		# 	del groups[keys[i]]
		# 	break
print group_counts['united states']

keys = groups.keys()
vals = [group_counts[key] for key in keys]

sorted = [list(x) for x in zip(*sorted(zip(keys, vals), key=lambda pair: pair[1]))]
keys = sorted[0][::-1]
vals = sorted[1][::-1]
with open('dict_collated_counts.txt','w') as f:
	for e,c in zip(keys,vals):
		#print(getEntities(groups[e]))
		f.write(getEntities(groups[e])+' = '+str(c))
		f.write('\n')
