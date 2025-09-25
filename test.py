
arr = [-1, 0, 1, 2, -1, -4]


def three_sum(arr):
    ans = []
    for i in range(0, len(arr)):
        s = 0
        elements = []
        for j in range(i, i + 3):
            s += arr[i]
            elements.append(arr[i])
        if s == 0:
            ans.append(elements)
    return ans    

print(three_sum(arr))
# Output: [[-1, -1, 2], [-1, 0, 1]]