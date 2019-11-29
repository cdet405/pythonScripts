number = int(input("Number : "))



if number >1:
    counter = 0
    while number !=1:
        counter += 1
        if (number % 2) == 0:
            number = int(number * .5)
        else:
            number = number *3+1
        print(number)
    print(f"count: {counter}")
else:
    print(":(")



# meme = 10
# while meme != 0:
#     print(meme)
#     meme = meme - 1



#print(number)
