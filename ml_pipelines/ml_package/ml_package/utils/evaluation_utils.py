
def evaluate_model(model, dataset, data_set_type = 'train'):
    metrics = model.evaluate(dataset, return_dict = True)
    list_metrics = []
    for metric in metrics:
        if 'loss' in metric:
            list_metrics.append({
                'name' : f"{data_set_type}_{metric}",
                'number_value' : float(metrics[metric]),
                'format' : "RAW"
            })
        else:
            list_metrics.append({
                'name' : f"{data_set_type}_{metric}",
                'number_value' : float(metrics[metric]),
                'format' : "PERCENTAGE"
            })
    return list_metrics

def evaluate_model_all_datasets(model, train_dataset, val_dataset, test_dataset):
    
    train_metrics = evaluate_model(model, train_dataset, 'train')
    val_metrics = evaluate_model(model, val_dataset, 'val')
    test_metrics = evaluate_model(model, test_dataset, 'test')
    
    return {
        'metrics' : train_metrics + val_metrics + test_metrics
    }

